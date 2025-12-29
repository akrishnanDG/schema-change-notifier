# Schema Change Notifier

A CLI tool that monitors Confluent Cloud audit logs for Schema Registry changes and produces structured notifications to a Kafka topic—enabling **event-driven reactions to schema evolution**.

## Why Use This Tool?

### The Problem

In event-driven architectures, **schemas are contracts**. When a schema changes, downstream systems often need to react:

- 🔄 **Data pipelines** need to update transformations for new fields
- 📊 **Analytics systems** need to refresh data models
- 📝 **Documentation** needs to regenerate API specs
- ✅ **Validation services** need to update their rules
- 🚨 **Alerting systems** need to notify data owners
- 🔗 **Dependent services** need to update their consumers

**The challenge:** Confluent Cloud provides audit logs, but they're buried in a dedicated cluster, mixed with thousands of other events, and lack the full schema content needed to take action.

### The Solution

This tool transforms raw audit log events into **actionable schema change notifications**:

```
┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
│   Schema Registry   │         │  Schema Change      │         │   Your Systems      │
│                     │         │  Notifier           │         │                     │
│  Developer updates  │────────▶│  • Filters events   │────────▶│  • CI/CD pipelines  │
│  a schema           │         │  • Fetches content  │         │  • Documentation    │
│                     │         │  • Produces Avro    │         │  • Data catalogs    │
└─────────────────────┘         └─────────────────────┘         │  • Alerting         │
                                                                └─────────────────────┘
```

**One notification topic, many consumers.** Each downstream system subscribes to `schema-change-notifications` and reacts in its own way.

### Use Cases

| Use Case | How It Works |
|----------|--------------|
| **Auto-generate documentation** | Consumer triggers OpenAPI/AsyncAPI spec generation on schema change |
| **Update data catalogs** | Push schema metadata to DataHub, Atlan, or custom catalogs |
| **Trigger CI/CD** | Kick off compatibility tests or deployment pipelines |
| **Notify data owners** | Send Slack/email alerts when critical schemas change |
| **Sync development environments** | Propagate schema changes across environments |
| **Audit & compliance** | Build a complete, queryable history of schema evolution |

## Features

- ✅ **Multi-environment support** - Monitor multiple Schema Registries across different environments
- ✅ **Avro serialization** - Strongly-typed notifications with Schema Registry integration
- ✅ **Processing modes** - Stream (real-time), Backfill (historical), Timestamp-based, Resume
- ✅ **Deduplication** - Prevents duplicate notifications for the same schema change
- ✅ **Subject filtering** - Monitor specific subjects using glob patterns
- ✅ **Fail-fast startup** - Validates configuration and registers schemas upfront

---

## Quick Start

### Prerequisites

- Java 17 or higher
- Maven 3.6+
- Confluent Cloud account with:
  - Access to audit log cluster
  - Schema Registry API keys for each environment to monitor
  - A Kafka cluster and topic for notifications

### Build

```bash
git clone <repository-url>
cd schema-change-notifier
mvn clean package -DskipTests
```

### Configure

Create `config/application.properties`:

```properties
# Audit Log Cluster (Confluent Cloud dedicated cluster for audit logs)
audit.log.bootstrap.servers=pkc-xxxxx.region.aws.confluent.cloud:9092
audit.log.api.key=YOUR_AUDIT_LOG_API_KEY
audit.log.api.secret=YOUR_AUDIT_LOG_API_SECRET

# Environment 1 - Schema Registry to monitor
environments.env-abc123.schema.registry.url=https://psrc-xxxxx.region.aws.confluent.cloud
environments.env-abc123.schema.registry.api.key=YOUR_SR_API_KEY
environments.env-abc123.schema.registry.api.secret=YOUR_SR_API_SECRET

# Target Cluster - Where notifications are produced
target.bootstrap.servers=pkc-yyyyy.region.aws.confluent.cloud:9092
target.api.key=YOUR_TARGET_API_KEY
target.api.secret=YOUR_TARGET_API_SECRET
target.topic=schema-change-notifications

# Target Schema Registry - For Avro serialization of notifications
target.schema.registry.url=https://psrc-yyyyy.region.aws.confluent.cloud
target.schema.registry.api.key=YOUR_TARGET_SR_API_KEY
target.schema.registry.api.secret=YOUR_TARGET_SR_API_SECRET
```

### Run

```bash
# Real-time monitoring (recommended for production)
java -jar target/schema-change-notifier-1.0.0.jar \
  --config config/application.properties \
  --mode STREAM

# See all options
java -jar target/schema-change-notifier-1.0.0.jar --help
```

---

## Processing Modes

### STREAM (Recommended for Production)

Starts from the latest offset and processes new events in real-time.

```bash
java -jar target/schema-change-notifier-1.0.0.jar --mode STREAM --config config/application.properties
```

**When to use:** Ongoing monitoring in production environments.

### BACKFILL

⚠️ **Warning:** Backfill processes ALL historical audit log events. Confluent Cloud retains audit logs for **7 days**, which can mean **millions of messages**. This may:
- Take hours to complete
- Consume significant network bandwidth
- Generate many API calls to Schema Registry

```bash
# Process all historical events
java -jar target/schema-change-notifier-1.0.0.jar --mode BACKFILL --config config/application.properties

# Process until current position, then stop
java -jar target/schema-change-notifier-1.0.0.jar --mode BACKFILL --stop-at-current --config config/application.properties
```

**When to use:** 
- Initial setup to catch up on historical changes
- Building a complete audit trail
- One-time migration scenarios

**Recommendations:**
- Use `--stop-at-current` to avoid running indefinitely
- Use a unique `--consumer-group` for backfill runs
- Run during off-peak hours
- Consider using TIMESTAMP mode for specific date ranges

### TIMESTAMP

Process events from a specific time range. Useful for targeted historical analysis.

```bash
java -jar target/schema-change-notifier-1.0.0.jar \
  --mode TIMESTAMP \
  --start-timestamp 2024-01-01T00:00:00Z \
  --end-timestamp 2024-01-31T23:59:59Z \
  --config config/application.properties
```

**When to use:** Analyzing changes during a specific incident or time period.

### RESUME

Continues from the last committed offset. Useful for restarting after failures.

```bash
java -jar target/schema-change-notifier-1.0.0.jar --mode RESUME --config config/application.properties
```

**When to use:** Restarting after a crash or maintenance.

---

## Configuration Reference

### Required Properties

| Property | Description |
|----------|-------------|
| `audit.log.bootstrap.servers` | Confluent Cloud audit log cluster |
| `audit.log.api.key` | API key for audit log cluster |
| `audit.log.api.secret` | API secret for audit log cluster |
| `environments.<env-id>.schema.registry.url` | Schema Registry URL for environment |
| `environments.<env-id>.schema.registry.api.key` | SR API key for environment |
| `environments.<env-id>.schema.registry.api.secret` | SR API secret for environment |
| `target.bootstrap.servers` | Target Kafka cluster for notifications |
| `target.api.key` | API key for target cluster |
| `target.api.secret` | API secret for target cluster |
| `target.topic` | Topic for notifications |
| `target.schema.registry.url` | Schema Registry for Avro serialization |
| `target.schema.registry.api.key` | SR API key for target |
| `target.schema.registry.api.secret` | SR API secret for target |

### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| `audit.log.topic` | `confluent-audit-log-events` | Audit log topic name |
| `consumer.group.id` | `schema-change-notifier` | Consumer group ID |
| `include.config.changes` | `false` | Include compatibility/mode changes |
| `only.successful` | `true` | Only process successful operations |
| `subject.filters` | (all) | Comma-separated subject patterns (e.g., `orders-*,payments-*`) |
| `state.file.path` | `./schema-change-notifier-state.json` | Deduplication state file |
| `dry.run` | `false` | Log but don't produce notifications |

### CLI Options

```
--config <file>        Path to configuration file (required)
--mode <mode>          Processing mode: STREAM, BACKFILL, TIMESTAMP, RESUME
--consumer-group <id>  Override consumer group ID
--start-timestamp <ts> Start timestamp for TIMESTAMP mode (ISO-8601)
--end-timestamp <ts>   End timestamp for TIMESTAMP mode (ISO-8601)
--stop-at-current      Stop when reaching current offset (BACKFILL mode)
--dry-run              Log notifications without producing
--help                 Show help message
--version              Show version
```

---

## Output Format

Notifications are produced as Avro messages with the following schema:

```json
{
  "event_type": "SCHEMA_REGISTERED",
  "environment_id": "env-abc123",
  "subject": "orders-value",
  "schema_id": 100001,
  "version": 3,
  "schema_type": "AVRO",
  "timestamp": "2024-01-15T10:30:00Z",
  "audit_log_event_id": "uuid-...",
  "data_contract_registered": {
    "schema": "{\"type\":\"record\",\"name\":\"Order\",...}",
    "references": null
  }
}
```

### Event Types

| Event Type | Trigger | Contains |
|------------|---------|----------|
| `SCHEMA_REGISTERED` | New schema version registered | Full schema content, version |
| `SCHEMA_DELETED` | Specific version soft-deleted | Version number |
| `SUBJECT_DELETED` | Subject deleted | Permanent flag |
| `COMPATIBILITY_UPDATED` | Compatibility level changed | New compatibility setting |
| `MODE_UPDATED` | Subject mode changed | New mode setting |

---

## Multi-Environment Setup

Monitor multiple environments by adding each to your configuration:

```properties
# Environment 1 (Production)
environments.env-prod123.schema.registry.url=https://psrc-prod.confluent.cloud
environments.env-prod123.schema.registry.api.key=PROD_KEY
environments.env-prod123.schema.registry.api.secret=PROD_SECRET

# Environment 2 (Staging)
environments.env-stg456.schema.registry.url=https://psrc-stg.confluent.cloud
environments.env-stg456.schema.registry.api.key=STG_KEY
environments.env-stg456.schema.registry.api.secret=STG_SECRET

# Environment 3 (Development)
environments.env-dev789.schema.registry.url=https://psrc-dev.confluent.cloud
environments.env-dev789.schema.registry.api.key=DEV_KEY
environments.env-dev789.schema.registry.api.secret=DEV_SECRET
```

The `environment_id` field in notifications identifies which environment the change occurred in.

---

## Best Practices

### Production Deployment

1. **Use STREAM mode** for ongoing monitoring
2. **Dedicated consumer group** per deployment
3. **Monitor the tool's health** via logs and metrics
4. **Set appropriate retention** on the notifications topic
5. **Use a service account** with minimal required permissions

### Consumer Group Management

```bash
# Use unique consumer groups for different purposes
--consumer-group schema-notifier-prod     # Production stream
--consumer-group schema-notifier-backfill # One-time backfill
--consumer-group schema-notifier-debug    # Testing/debugging
```

### Subject Filtering

Reduce noise by monitoring only relevant subjects:

```properties
# Monitor specific subjects
subject.filters=orders-*,payments-*,users-value

# Or via CLI
--subject-filters "orders-*,payments-*"
```

### Deduplication

The tool automatically deduplicates events using `subject + operation + schemaId`. The state is persisted to disk for crash recovery. Each unique schema version is processed exactly once.

---

## Logging Configuration

The tool uses Logback for logging. To adjust log levels, edit `src/main/resources/logback.xml`.

### Reduce Verbose Kafka/Confluent Logging

By default, Kafka serializers log their full configuration at startup. To suppress this:

```xml
<!-- Add to logback.xml -->
<logger name="io.confluent.kafka.serializers" level="WARN"/>
<logger name="org.apache.kafka" level="WARN"/>
```

### Enable Debug Logging

For troubleshooting, enable debug logs for the application:

```xml
<logger name="io.confluent.schemachange" level="DEBUG"/>
```

### Log to File

To also log to a file:

```xml
<appender name="FILE" class="ch.qos.logback.core.FileAppender">
    <file>logs/schema-change-notifier.log</file>
    <encoder>
        <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
</appender>

<root level="INFO">
    <appender-ref ref="CONSOLE"/>
    <appender-ref ref="FILE"/>
</root>
```

After modifying `logback.xml`, rebuild with `mvn package -DskipTests`.

---

## Troubleshooting

### No events being processed

1. **Check environment IDs** - Ensure your configured environment IDs match those in audit logs
2. **Verify credentials** - Test Schema Registry API access independently
3. **Check audit log access** - Ensure you can consume from `confluent-audit-log-events`

### High latency in processing

1. **Network proximity** - Run the tool close to the Confluent Cloud region
2. **Batch size** - Increase `batch.size` for higher throughput
3. **Schema Registry caching** - Schemas are cached to reduce API calls

### Memory issues with backfill

1. **Limit date range** - Use TIMESTAMP mode instead
2. **Increase heap** - `java -Xmx2g -jar ...`
3. **Use stop-at-current** - Prevent indefinite processing

### Duplicate notifications

1. **Check consumer group** - Using the same group across restarts preserves offsets
2. **State file** - Ensure `state.file.path` is writable and persisted

---

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│  Confluent Cloud    │     │  Schema Change       │     │  Target Kafka       │
│  Audit Log Cluster  │────▶│  Notifier            │────▶│  Cluster            │
│                     │     │                      │     │                     │
│ confluent-audit-    │     │  ┌────────────────┐  │     │ schema-change-      │
│ log-events          │     │  │ Filter         │  │     │ notifications       │
└─────────────────────┘     │  │ Schema Fetch   │  │     └─────────────────────┘
                            │  │ Dedup          │  │
┌─────────────────────┐     │  │ Transform      │  │     ┌─────────────────────┐
│  Schema Registries  │◀───▶│  └────────────────┘  │     │  Target Schema      │
│  (per environment)  │     │                      │     │  Registry           │
└─────────────────────┘     └──────────────────────┘     └─────────────────────┘
```

---

## Development

### Building

```bash
mvn clean package
```

### Running Tests

```bash
mvn test
```

### Code Structure

```
src/main/java/io/confluent/schemachange/
├── SchemaChangeNotifierApp.java    # Main entry point
├── cli/                            # Command-line interface
├── config/                         # Configuration classes
├── consumer/                       # Audit log consumer
├── exception/                      # Custom exceptions
├── model/                          # Data models
├── processor/                      # Event processing logic
├── producer/                       # Notification producer
├── registry/                       # Schema Registry client
├── state/                          # Deduplication state
└── util/                           # Utilities
```

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.
