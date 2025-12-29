# Contributing to Schema Change Notifier

Thank you for your interest in contributing! This project follows an open source model and welcomes contributions from the community.

## How to Contribute

### Reporting Issues

- **Bug reports**: Open an issue with a clear description, steps to reproduce, and expected vs actual behavior
- **Feature requests**: Open an issue describing the use case and proposed solution
- **Questions**: Use discussions or issues for questions about usage

### Submitting Changes

1. **Fork the repository**

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Follow existing code style and conventions
   - Add/update tests for new functionality
   - Update documentation as needed

4. **Run tests**
   ```bash
   mvn clean test
   ```

5. **Commit with a clear message**
   ```bash
   git commit -m "feat: add support for X"
   ```
   
   Follow [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` new feature
   - `fix:` bug fix
   - `docs:` documentation only
   - `test:` adding/updating tests
   - `refactor:` code change that neither fixes a bug nor adds a feature

6. **Push and create a Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

### Pull Request Guidelines

- Keep PRs focused on a single change
- Include tests for new functionality
- Update README.md if adding new features or configuration options
- Ensure all tests pass
- Respond to review feedback promptly

## Development Setup

### Prerequisites

- Java 17 or higher
- Maven 3.6+

### Building

```bash
mvn clean package
```

### Running Tests

```bash
mvn test
```

### Code Style

- Use 4 spaces for indentation (no tabs)
- Follow standard Java naming conventions
- Add Javadoc to public classes and methods
- Use `@Nonnull` / `@Nullable` annotations for null safety

### Project Structure

```
src/main/java/io/confluent/schemachange/
├── cli/          # Command-line interface
├── config/       # Configuration classes
├── consumer/     # Kafka consumer for audit logs
├── exception/    # Custom exceptions
├── model/        # Data models
├── processor/    # Event processing logic
├── producer/     # Kafka producer for notifications
├── registry/     # Schema Registry client
├── state/        # Deduplication state management
└── util/         # Utilities
```

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

