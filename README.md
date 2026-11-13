# kafka-offset-reset

Moves a consumer group's committed offsets on a topic to `latest`, `earliest` or a point in
time, the same as `kafka-consumer-groups.sh --reset-offsets --execute`, as a Vizion Hub module.

The default schedule never fires: click **Run** on the module when you need a reset.

## How to use
1. Stop (disable) the kafka-bulk-consumer module that reads the topic and wait for its run to end.
2. Put the topic and group in `targets`, then run this module.
3. Read the module log: one line per target with before/after offsets per partition.
4. Start the consumer module again.

## Safety
- Refuses a group that still has active members, with their hosts in the error. A live
  consumer would commit over the reset, and the broker rejects it anyway.
- Reads the offsets back after the change and fails if they differ.
- Only changes offsets for the topics listed. Never consumes or produces messages.

## Settings
Auth settings have the same names as kafka-bulk-consumer.

| setting | meaning |
|---|---|
| `targets[].topic` | topic to reset |
| `targets[].groupId` | consumer group (the module's `kafkaGroupId`) |
| `targets[].to` | `latest` (default), `earliest`, or an ISO time such as `2026-11-13T00:00:00Z` (UTC when no zone): the first offset at or after it, or latest when no message is that new |
| `adminTimeoutSec` | broker request timeout, default 30 |

Kafka ACLs needed: DESCRIBE and READ on the group, DESCRIBE and READ on the topic. A consumer
that already reads the topic has these.
