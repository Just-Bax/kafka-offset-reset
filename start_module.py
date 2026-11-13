#!/usr/bin/env python3
"""
kafka-offset-reset: move a consumer group's committed offsets on a topic, the same as
`kafka-consumer-groups.sh --reset-offsets --execute`, from a Vizion Hub module.

Per target (topic + groupId + to):
  1. refuse if the group has active members: their next commit would overwrite the reset,
     and the broker rejects offset changes for a live group anyway
  2. look up the new offset of every partition: latest, earliest, or the first offset
     at or after a timestamp (latest when nothing is that new)
  3. set the group's offsets with the AdminClient, read them back and log before/after

Stop the consumer module first, run this one, then start the consumer again.
"""

import json
import os
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path

try:
    from confluent_kafka import ConsumerGroupState, ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec
    from onevizion import LogLevel, ModuleLog
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
    os.execv(sys.executable, [sys.executable] + sys.argv)

MODULE_VERSION = '1.0'
IDLE_STATES = (ConsumerGroupState.EMPTY, ConsumerGroupState.DEAD)
NO_OFFSET = -1001

_cert_files = []


# ---------------------------------------------------------------- logging

class Log:
    def __init__(self, module_log):
        self._log = module_log

    def info(self, message, data=None):
        desc = json.dumps(data, default=str, indent=1) if data is not None else None
        print(message)
        if desc:
            print(desc)
        self._log.add(LogLevel.INFO, message[:2000], desc)

    def error(self, message):
        print(message, file=sys.stderr)
        self._log.add(LogLevel.ERROR, message[:2000], traceback.format_exc())


# ---------------------------------------------------------------- kafka config

def write_cert(content, name):
    f = tempfile.NamedTemporaryFile('w', delete=False, suffix=f'_{name}.pem')
    f.write(content)
    f.close()
    _cert_files.append(Path(f.name))
    return f.name


def kafka_auth_config(s):
    """Same auth options and setting names as kafka-bulk-consumer."""
    cfg = {'bootstrap.servers': s['kafkaBootstrapServers']}
    auth = s.get('kafkaAuthType', 'PLAINTEXT')
    if auth == 'PLAINTEXT':
        cfg['security.protocol'] = 'PLAINTEXT'
    elif auth == 'SASL':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SASL_PLAINTEXT',
            'sasl.mechanism': s.get('kafkaSaslMechanism') or 'PLAIN',
            'sasl.username': s['kafkaSaslUsername'],
            'sasl.password': s['kafkaSaslPassword'],
        })
    elif auth == 'SASL_OAUTH2':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SASL_SSL',
            'sasl.mechanism': s.get('kafkaSaslMechanism') or 'OAUTHBEARER',
            'sasl.oauthbearer.method': s.get('kafkaSaslOauthbearerMethod') or 'oidc',
            'sasl.oauthbearer.token.endpoint.url': s['kafkaSaslOauthbearerTokenEndpointUrl'],
            'sasl.oauthbearer.client.id': s['kafkaSaslOauthbearerClientId'],
            'sasl.oauthbearer.client.secret': s['kafkaSaslOauthbearerClientSecret'],
        })
        if s.get('kafkaSaslOauthbearerScope'):
            cfg['sasl.oauthbearer.scope'] = s['kafkaSaslOauthbearerScope']
    elif auth == 'M_TLS':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SSL',
            'ssl.ca.location': write_cert(s['kafkaSslCa'], 'ca'),
            'ssl.certificate.location': write_cert(s['kafkaSslCert'], 'cert'),
            'ssl.key.location': write_cert(s['kafkaSslKey'], 'key'),
        })
        if s.get('kafkaSslKeyPassword'):
            cfg['ssl.key.password'] = s['kafkaSslKeyPassword']
    else:
        raise ValueError(f'Unknown kafkaAuthType: {auth}')

    if cfg.get('security.protocol') in ('SASL_SSL', 'SSL'):
        cfg['ssl.endpoint.identification.algorithm'] = 'https'
        if auth != 'M_TLS' and (s.get('kafkaSslCa') or '').strip():
            cfg['ssl.ca.location'] = write_cert(s['kafkaSslCa'], 'ca')
    return cfg


# ---------------------------------------------------------------- reset

def offset_spec(to):
    """'latest', 'earliest' or an ISO date/time such as 2026-11-13T00:00:00Z (UTC if no zone)."""
    if to == 'latest':
        return OffsetSpec.latest()
    if to == 'earliest':
        return OffsetSpec.earliest()
    ts = datetime.fromisoformat(to.replace('Z', '+00:00'))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return OffsetSpec.for_timestamp(int(ts.timestamp() * 1000))


def check_group_idle(admin, group, timeout):
    try:
        d = admin.describe_consumer_groups([group], request_timeout=timeout)[group].result()
    except KafkaException as e:
        if e.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
            return 'not found'
        raise
    if d.state not in IDLE_STATES or d.members:
        hosts = sorted({m.host for m in d.members})
        raise RuntimeError(f'group {group} is {d.state.name} with {len(d.members)} active member(s) '
                           f'on {hosts}: stop the consumer module, then run this module again')
    return d.state.name


def partitions_of(admin, topic, timeout):
    t = admin.list_topics(topic=topic, timeout=timeout).topics.get(topic)
    if t is None or t.error is not None or not t.partitions:
        raise RuntimeError(f'topic {topic} not found or not readable: {t.error if t else "missing"}')
    return sorted(t.partitions)


def list_offsets(admin, topic, partitions, spec, timeout):
    futures = admin.list_offsets({TopicPartition(topic, p): spec for p in partitions},
                                 request_timeout=timeout)
    return {tp.partition: f.result().offset for tp, f in futures.items()}


def committed(admin, group, topic, partitions, timeout):
    req = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in partitions])
    res = admin.list_consumer_group_offsets([req], request_timeout=timeout)[group].result()
    return {tp.partition: tp.offset for tp in res.topic_partitions}


def reset_target(log, admin, target, timeout):
    topic, group = target['topic'], target['groupId']
    to = str(target.get('to') or 'latest').strip()
    spec = offset_spec(to)

    state = check_group_idle(admin, group, timeout)
    partitions = partitions_of(admin, topic, timeout)

    new = list_offsets(admin, topic, partitions, spec, timeout)
    too_new = [p for p, o in new.items() if o < 0]
    if too_new:
        new.update(list_offsets(admin, topic, too_new, OffsetSpec.latest(), timeout))

    before = committed(admin, group, topic, partitions, timeout)

    req = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p, new[p]) for p in partitions])
    res = admin.alter_consumer_group_offsets([req], request_timeout=timeout)[group].result()
    errors = [f'partition {tp.partition}: {tp.error}' for tp in res.topic_partitions if tp.error]
    if errors:
        raise RuntimeError(f'group {group} on {topic}: broker rejected the reset: {errors}')

    after = committed(admin, group, topic, partitions, timeout)
    rows = [{'partition': p,
             'before': before.get(p) if before.get(p) != NO_OFFSET else None,
             'after': after.get(p),
             'skipped': new[p] - before[p] if before.get(p, NO_OFFSET) >= 0 else None}
            for p in partitions]
    wrong = [r['partition'] for r in rows if r['after'] != new[r['partition']]]
    if wrong:
        raise RuntimeError(f'group {group} on {topic}: read back differs on partitions {wrong}: {rows}')

    skipped = sum(r['skipped'] or 0 for r in rows)
    log.info(f'{group} on {topic}: reset to {to} on {len(partitions)} partition(s), '
             f'{skipped} message(s) skipped (negative = will be re-read), group was {state}',
             {'topic': topic, 'group': group, 'to': to, 'partitions': rows})


# ---------------------------------------------------------------- main

def main():
    with open('settings.json') as f:
        settings = json.load(f)
    params_file = Path('ihub_parameters.json')
    if params_file.exists():
        with open(params_file) as f:
            run = json.load(f)
        module_log = ModuleLog(run['processId'], run['ovUrl'], settings['ovAccessKey'],
                               settings['ovSecretKey'], None, True, run.get('logLevel', 'info'))
    else:
        class _Print:
            def add(self, level, message, description=None):
                pass
        module_log = _Print()

    log = Log(module_log)
    timeout = float(settings.get('adminTimeoutSec', 30))
    targets = settings['targets']
    log.info(f'kafka-offset-reset {MODULE_VERSION}, {len(targets)} target(s)')

    admin = AdminClient(kafka_auth_config(settings))
    failures = 0
    try:
        for target in targets:
            try:
                reset_target(log, admin, target, timeout)
            except Exception as e:
                failures += 1
                log.error(f'{target.get("groupId")} on {target.get("topic")}: {e}')
    finally:
        for p in _cert_files:
            try:
                p.unlink()
            except Exception:
                pass

    log.info(f'finished, {failures} target(s) failed')
    if failures:
        sys.exit(1)


if __name__ == '__main__':
    main()
