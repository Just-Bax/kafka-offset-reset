# kafka-offset-reset

Moves a consumer group's committed offsets on a topic to `latest`, `earliest` or a point in
time, the same as `kafka-consumer-groups.sh --reset-offsets --execute`, as a Vizion Hub module.

Modules only run on their schedule (default every 5 minutes while enabled). The module resets
each target **once per `resetId`** and skips it on later runs, so leaving it enabled does not
keep moving the offsets.

## How to reset
1. Disable the kafka-bulk-consumer module that reads the topic, and wait until its current run ends.
2. In this module's settings put the topic and group in `targets` and set a new `resetId`
   (any text not used before, for example `2026-11-13-1`).
3. Enable this module and wait for its next run (at most 5 minutes).
4. Check the module log: one line per target with before, after and skipped messages per partition,
   then `finished, 0 target(s) failed: disable this module`.
5. Disable this module, then enable the consumer module again.

To reset the same topic again later, change `resetId`. Later runs with the same `resetId` only
log `nothing to do`.

## When a run fails
- `group ... is STABLE with N active member(s) on [hosts]`: the consumer still runs. Stop it;
  the target is tried again on the next run.
- Targets that succeeded are not reset again on the retry; only the failed ones are.
- The run ends with an error (exit 1) and the cause in the module log.

## Safety
- Refuses a group that still has active members. A live consumer would commit over the reset,
  and the broker rejects it anyway.
- Reads the offsets back after the change and fails if they differ.
- Only changes offsets for the topics and groups listed. Never consumes or produces messages.

## Settings
Auth settings have the same names and options as kafka-bulk-consumer: copy them from its settings.

| setting | meaning |
|---|---|
| `resetId` | required; any text. Each target is reset once per value; change it for a new reset |
| `targets[].topic` | topic to reset |
| `targets[].groupId` | consumer group (the consumer module's `kafkaGroupId`) |
| `targets[].to` | `latest` (default), `earliest`, or an ISO time such as `2026-11-13T00:00:00Z` (UTC when no zone): the first offset at or after it, or latest when no message is that new |
| `adminTimeoutSec` | broker request timeout in seconds, default 30 |

Kafka ACLs needed: DESCRIBE and READ on the group, DESCRIBE and READ on the topic. A consumer
that already reads the topic has these.

Done targets are kept in `reset_done.json` in the module's working directory. If that folder is
wiped (module reinstalled), the current `resetId` can run once more.
