# Kafka topics

| Topic | Role | Partitions | Used by |
|-------|------|-----------|---------|
| `txn-source` | **source** — bank transaction events | 6 | UC-1 producer → UC-1 RTM pipeline |
| `events-source` | **source** — generic events | 6 | UC-2 producer → UC-2 Iceberg pipeline |
| `fraud-scored` | **sink** — RTM-scored transactions | 6 | UC-1 RTM pipeline output |
| `events-dlq` | **sink** — schema-validation failures | 3 | UC-2 pipeline dead-letter |

Partition count 6 matches the pipelines' `minPartitions=6` read option for optimal parallelism.
Replication factor is 1 (single broker, throwaway test). For any durable/prod setup, use
≥3 brokers and RF=3.

## Manual topic ops (from the EC2 host)

```bash
KAFKA=/opt/kafka; BOOT=<host>:9094; C=/opt/kafka/config/client.properties
# list
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BOOT --command-config $C --list
# describe
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BOOT --command-config $C --describe --topic txn-source
# tail a sink to eyeball output
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BOOT --consumer.config $C \
  --topic fraud-scored --from-beginning --max-messages 5
```
