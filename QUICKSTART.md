# Quick Start Guide

Get the Schema Change Notifier running in 5 minutes.

> **What is this?** A tool that watches for schema changes in Confluent Cloud and produces notifications to a Kafka topic—so your systems can **react to schema evolution** automatically.

## 1. Build

```bash
mvn clean package -DskipTests
```

## 2. Get Your Credentials

You'll need credentials from Confluent Cloud:

| What | Where to Find |
|------|---------------|
| **Audit Log Cluster** | Confluent Cloud → Organization Settings → Audit Log |
| **Schema Registry (per env)** | Environment → Schema Registry → API Keys |
| **Target Kafka Cluster** | Cluster → API Keys |

## 3. Create Config File

Create `config/application.properties`:

```properties
# ===== AUDIT LOG CLUSTER =====
# Find in: Organization Settings → Audit Log → Access
audit.log.bootstrap.servers=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
audit.log.api.key=<AUDIT_API_KEY>
audit.log.api.secret=<AUDIT_API_SECRET>

# ===== ENVIRONMENTS TO MONITOR =====
# Add one block per environment. Find env ID in the URL when viewing the environment.
# Example: https://confluent.cloud/environments/env-abc123 → env-abc123

environments.env-abc123.schema.registry.url=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
environments.env-abc123.schema.registry.api.key=<SR_API_KEY>
environments.env-abc123.schema.registry.api.secret=<SR_API_SECRET>

# ===== TARGET CLUSTER =====
# Where notifications will be produced
target.bootstrap.servers=pkc-yyyyy.us-east-2.aws.confluent.cloud:9092
target.api.key=<TARGET_API_KEY>
target.api.secret=<TARGET_API_SECRET>
target.topic=schema-change-notifications

# ===== TARGET SCHEMA REGISTRY =====
# For Avro serialization of notification messages
target.schema.registry.url=https://psrc-yyyyy.us-east-2.aws.confluent.cloud
target.schema.registry.api.key=<TARGET_SR_API_KEY>
target.schema.registry.api.secret=<TARGET_SR_API_SECRET>
```

## 4. Create Target Topic

In Confluent Cloud Console, create the notifications topic:
- Topic name: `schema-change-notifications`
- Partitions: 3 (adjust as needed)
- Retention: 7 days (or as required)

## 5. Run

```bash
# Start in streaming mode (real-time)
java -jar target/schema-change-notifier-1.0.0.jar \
  --config config/application.properties \
  --mode STREAM
```

You should see:
```
Starting Schema Change Notifier in STREAM mode...
Schema registered successfully: subject=schema-change-notifications-value, schemaId=...
Setup complete, listening for schema changes in audit logs...
```

## 6. Test It

1. Go to Confluent Cloud → Your Environment → Schema Registry
2. Create or update any schema
3. Watch the notifier logs for:
   ```
   Produced SCHEMA_REGISTERED notification for subject orders-value (schema_id=100001)
   ```
4. Check your target topic for the notification message

---

## Common Commands

```bash
# Real-time monitoring
java -jar target/schema-change-notifier-1.0.0.jar --mode STREAM --config config/application.properties

# Dry run (log without producing)
java -jar target/schema-change-notifier-1.0.0.jar --mode STREAM --dry-run --config config/application.properties

# Backfill historical data (⚠️ can be millions of events!)
java -jar target/schema-change-notifier-1.0.0.jar --mode BACKFILL --stop-at-current --config config/application.properties

# Process specific time range
java -jar target/schema-change-notifier-1.0.0.jar --mode TIMESTAMP \
  --start-timestamp 2024-01-01T00:00:00Z \
  --end-timestamp 2024-01-02T00:00:00Z \
  --config config/application.properties
```

---

## Troubleshooting

### "No environments configured"
Add at least one environment block to your config:
```properties
environments.env-XXXXX.schema.registry.url=...
environments.env-XXXXX.schema.registry.api.key=...
environments.env-XXXXX.schema.registry.api.secret=...
```

### "Schema not found in environment"
The environment ID in your config must match the one in audit logs. Check:
1. Go to Confluent Cloud
2. Look at the URL: `https://confluent.cloud/environments/env-XXXXX`
3. Use exactly `env-XXXXX` in your config

### "Connection refused" / "Authentication failed"
1. Verify credentials are correct
2. Ensure API keys have appropriate permissions
3. Check network connectivity to Confluent Cloud

### "0 events processed" in STREAM mode
This is normal if no schema changes are happening. Register/update a schema to test.

---

## What's Next?

Now that notifications are flowing, build consumers that react to schema changes:

| Build This | To Do This |
|------------|------------|
| **Slack/Teams bot** | Alert data owners when their schemas change |
| **CI/CD trigger** | Run compatibility tests on schema updates |
| **Doc generator** | Auto-update AsyncAPI specs |
| **Data catalog sync** | Push schema metadata to DataHub/Atlan |
| **Audit logger** | Store schema history in a database |

Each consumer subscribes to `schema-change-notifications` and filters for relevant events.

**Example:** A simple Slack notifier might filter for `event_type=SCHEMA_REGISTERED` and post:
> 📦 Schema `orders-value` updated to version 3 in `env-prod123`

Read the full [README.md](README.md) for advanced configuration and multi-environment setup.

