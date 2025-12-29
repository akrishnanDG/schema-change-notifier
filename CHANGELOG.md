# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-12-29

### Added

- **Core Functionality**
  - Monitor Confluent Cloud audit logs for Schema Registry changes
  - Produce Avro-serialized notifications to a Kafka topic
  - Support for multiple Schema Registry environments

- **Processing Modes**
  - `STREAM` - Real-time processing from latest offset
  - `BACKFILL` - Process all historical events
  - `TIMESTAMP` - Process events within a time range
  - `RESUME` - Continue from last committed offset

- **Event Types**
  - `SCHEMA_REGISTERED` - New schema version registered
  - `SCHEMA_DELETED` - Specific version deleted
  - `SUBJECT_DELETED` - Subject deleted
  - `COMPATIBILITY_UPDATED` - Compatibility level changed
  - `MODE_UPDATED` - Subject mode changed

- **Features**
  - Automatic deduplication of events
  - Subject filtering with glob patterns
  - Upfront schema registration (fail-fast)
  - Configurable via properties file or CLI
  - Dry-run mode for testing

- **Documentation**
  - Comprehensive README with architecture diagram
  - Quick start guide
  - Sample configuration file
  - Javadoc on public APIs

- **Testing**
  - 91 unit tests covering core functionality
  - Custom exceptions for better error handling
  - Java 23 compatible test setup

### Technical Details

- Built with Java 17+
- Uses Confluent Kafka clients 7.5.0
- Avro serialization with Schema Registry
- Picocli for CLI argument parsing
- Logback for logging

