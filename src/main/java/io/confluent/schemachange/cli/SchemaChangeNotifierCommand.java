package io.confluent.schemachange.cli;

import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.config.EnvironmentConfig;
import io.confluent.schemachange.config.ProcessingMode;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CLI command definition using picocli.
 */
@Command(
        name = "schema-change-notifier",
        mixinStandardHelpOptions = true,
        version = "1.0.0",
        description = "Monitor Confluent Cloud audit logs for schema changes and produce notifications"
)
public class SchemaChangeNotifierCommand implements Callable<Integer> {

    // Config file option
    @Option(names = {"-c", "--config"}, description = "Path to configuration file")
    private String configFile;

    // Audit Log Cluster options
    @Option(names = {"--audit-bootstrap-servers"}, description = "Audit log cluster bootstrap servers")
    private String auditBootstrapServers;

    @Option(names = {"--audit-api-key"}, description = "Audit log cluster API key")
    private String auditApiKey;

    @Option(names = {"--audit-api-secret"}, description = "Audit log cluster API secret")
    private String auditApiSecret;

    // Note: Schema Registry is now configured per environment in the config file
    // Use environments.<env-id>.schema.registry.url, etc.

    // Target Cluster options
    @Option(names = {"--target-bootstrap-servers"}, description = "Target cluster bootstrap servers")
    private String targetBootstrapServers;

    @Option(names = {"--target-api-key"}, description = "Target cluster API key")
    private String targetApiKey;

    @Option(names = {"--target-api-secret"}, description = "Target cluster API secret")
    private String targetApiSecret;

    @Option(names = {"--output-topic", "-o"}, description = "Output topic for notifications")
    private String outputTopic;

    // Processing mode options
    @Option(names = {"--mode", "-m"}, description = "Processing mode: STREAM, BACKFILL, TIMESTAMP, RESUME")
    private ProcessingMode processingMode;

    @Option(names = {"--start-timestamp"}, description = "Start timestamp for TIMESTAMP mode (ISO-8601)")
    private String startTimestamp;

    @Option(names = {"--end-timestamp"}, description = "End timestamp for TIMESTAMP mode (ISO-8601)")
    private String endTimestamp;

    @Option(names = {"--stop-at-current"}, description = "Stop after reaching current end of topic (for BACKFILL)")
    private boolean stopAtCurrent;

    // Consumer options
    @Option(names = {"--consumer-group"}, description = "Consumer group ID")
    private String consumerGroup;

    // Filtering options
    @Option(names = {"--include-methods"}, description = "Comma-separated list of methods to include")
    private String includeMethods;

    @Option(names = {"--include-config-changes"}, description = "Include compatibility and mode updates")
    private boolean includeConfigChanges;

    @Option(names = {"--filter-subjects"}, description = "Comma-separated list of subject patterns to filter")
    private String filterSubjects;

    // Deduplication options
    @Option(names = {"--enable-deduplication"}, description = "Enable event deduplication", defaultValue = "true")
    private boolean enableDeduplication;

    @Option(names = {"--state-file"}, description = "Path to state file for deduplication")
    private String stateFile;

    // Security options
    @Option(names = {"--security-protocol"}, description = "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    private String securityProtocol;

    @Option(names = {"--sasl-mechanism"}, description = "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER")
    private String saslMechanism;

    // Health server options
    @Option(names = {"--health-port"}, description = "Health check server port (0 to disable)")
    private Integer healthPort;

    // Processing threads
    @Option(names = {"--processing-threads"}, description = "Number of threads for parallel event processing")
    private Integer processingThreads;

    // Operational options
    @Option(names = {"--dry-run"}, description = "Print notifications without producing")
    private boolean dryRun;

    @Option(names = {"--poll-timeout"}, description = "Poll timeout in milliseconds")
    private Integer pollTimeout;

    @Option(names = {"--batch-size"}, description = "Maximum records per poll")
    private Integer batchSize;

    // Callback for running the application
    private Runnable applicationRunner;

    public void setApplicationRunner(Runnable runner) {
        this.applicationRunner = runner;
    }

    @Override
    public Integer call() throws Exception {
        if (applicationRunner != null) {
            applicationRunner.run();
        }
        return 0;
    }

    /**
     * Build AppConfig from CLI arguments and/or config file.
     */
    public AppConfig buildConfig() throws IOException {
        AppConfig config = new AppConfig();

        // Load from config file first if provided
        if (configFile != null) {
            loadFromConfigFile(config, configFile);
        }

        // Override with CLI arguments (CLI takes precedence)
        applyCliOverrides(config);

        return config;
    }

    // Pattern to match environment-specific properties: environments.<env-id>.<property>
    private static final Pattern ENV_PROPERTY_PATTERN = 
            Pattern.compile("environments\\.([^.]+)\\.(.+)");

    private void loadFromConfigFile(AppConfig config, String filePath) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            props.load(fis);
        }

        // Audit Log Cluster
        if (props.containsKey("audit.log.bootstrap.servers")) {
            config.setAuditLogBootstrapServers(props.getProperty("audit.log.bootstrap.servers"));
        }
        if (props.containsKey("audit.log.api.key")) {
            config.setAuditLogApiKey(props.getProperty("audit.log.api.key"));
        }
        if (props.containsKey("audit.log.api.secret")) {
            config.setAuditLogApiSecret(props.getProperty("audit.log.api.secret"));
        }
        if (props.containsKey("audit.log.topic")) {
            config.setAuditLogTopic(props.getProperty("audit.log.topic"));
        }

        // Load environment-specific Schema Registry configurations
        loadEnvironmentConfigs(config, props);

        // Target Cluster
        if (props.containsKey("target.bootstrap.servers")) {
            config.setTargetBootstrapServers(props.getProperty("target.bootstrap.servers"));
        }
        if (props.containsKey("target.api.key")) {
            config.setTargetApiKey(props.getProperty("target.api.key"));
        }
        if (props.containsKey("target.api.secret")) {
            config.setTargetApiSecret(props.getProperty("target.api.secret"));
        }
        if (props.containsKey("target.topic")) {
            config.setTargetTopic(props.getProperty("target.topic"));
        }

        // Target Schema Registry (for notification schema)
        if (props.containsKey("target.schema.registry.url")) {
            config.setTargetSchemaRegistryUrl(props.getProperty("target.schema.registry.url"));
        }
        if (props.containsKey("target.schema.registry.api.key")) {
            config.setTargetSchemaRegistryApiKey(props.getProperty("target.schema.registry.api.key"));
        }
        if (props.containsKey("target.schema.registry.api.secret")) {
            config.setTargetSchemaRegistryApiSecret(props.getProperty("target.schema.registry.api.secret"));
        }

        // Processing Mode
        if (props.containsKey("processing.mode")) {
            config.setProcessingMode(ProcessingMode.valueOf(props.getProperty("processing.mode").toUpperCase()));
        }
        if (props.containsKey("start.timestamp")) {
            config.setStartTimestamp(props.getProperty("start.timestamp"));
        }
        if (props.containsKey("end.timestamp")) {
            config.setEndTimestamp(props.getProperty("end.timestamp"));
        }
        if (props.containsKey("stop.at.current")) {
            config.setStopAtCurrent(Boolean.parseBoolean(props.getProperty("stop.at.current")));
        }

        // Consumer
        if (props.containsKey("consumer.group.id")) {
            config.setConsumerGroupId(props.getProperty("consumer.group.id"));
        }

        // Filtering
        if (props.containsKey("filter.method.names")) {
            String methods = props.getProperty("filter.method.names");
            config.setIncludeMethods(new HashSet<>(Arrays.asList(methods.split(","))));
        }
        if (props.containsKey("include.config.changes")) {
            config.setIncludeConfigChanges(Boolean.parseBoolean(props.getProperty("include.config.changes")));
        }
        if (props.containsKey("filter.subjects")) {
            String subjects = props.getProperty("filter.subjects");
            config.setSubjectFilters(new HashSet<>(Arrays.asList(subjects.split(","))));
        }

        // Deduplication
        if (props.containsKey("enable.deduplication")) {
            config.setEnableDeduplication(Boolean.parseBoolean(props.getProperty("enable.deduplication")));
        }
        if (props.containsKey("state.store.path")) {
            config.setStateFilePath(props.getProperty("state.store.path"));
        }

        // Security
        if (props.containsKey("security.protocol")) {
            config.setSecurityProtocol(props.getProperty("security.protocol"));
        }
        if (props.containsKey("sasl.mechanism")) {
            config.setSaslMechanism(props.getProperty("sasl.mechanism"));
        }

        // Health server
        if (props.containsKey("health.port")) {
            config.setHealthPort(Integer.parseInt(props.getProperty("health.port")));
        }

        // Processing threads
        if (props.containsKey("processing.threads")) {
            config.setProcessingThreads(Integer.parseInt(props.getProperty("processing.threads")));
        }

        // Operational
        if (props.containsKey("dry.run")) {
            config.setDryRun(Boolean.parseBoolean(props.getProperty("dry.run")));
        }
        if (props.containsKey("poll.timeout.ms")) {
            config.setPollTimeoutMs(Integer.parseInt(props.getProperty("poll.timeout.ms")));
        }
        if (props.containsKey("batch.size")) {
            config.setBatchSize(Integer.parseInt(props.getProperty("batch.size")));
        }
    }

    /**
     * Load environment-specific Schema Registry configurations.
     * 
     * Properties format:
     *   environments.<env-id>.schema.registry.url=https://...
     *   environments.<env-id>.schema.registry.api.key=...
     *   environments.<env-id>.schema.registry.api.secret=...
     */
    private void loadEnvironmentConfigs(AppConfig config, Properties props) {
        // Collect all environment IDs
        Set<String> envIds = new HashSet<>();
        for (String key : props.stringPropertyNames()) {
            Matcher matcher = ENV_PROPERTY_PATTERN.matcher(key);
            if (matcher.matches()) {
                envIds.add(matcher.group(1));
            }
        }

        // Load each environment configuration
        for (String envId : envIds) {
            String prefix = "environments." + envId + ".";
            
            String url = props.getProperty(prefix + "schema.registry.url");
            String apiKey = props.getProperty(prefix + "schema.registry.api.key");
            String apiSecret = props.getProperty(prefix + "schema.registry.api.secret");

            if (url != null && apiKey != null && apiSecret != null) {
                EnvironmentConfig envConfig = new EnvironmentConfig(envId, url, apiKey, apiSecret);
                config.addEnvironment(envConfig);
            }
        }
    }

    private void applyCliOverrides(AppConfig config) {
        // Audit Log Cluster
        if (auditBootstrapServers != null) {
            config.setAuditLogBootstrapServers(auditBootstrapServers);
        }
        if (auditApiKey != null) {
            config.setAuditLogApiKey(auditApiKey);
        }
        if (auditApiSecret != null) {
            config.setAuditLogApiSecret(auditApiSecret);
        }

        // Note: Schema Registry configs are per-environment and loaded from config file only

        // Target Cluster
        if (targetBootstrapServers != null) {
            config.setTargetBootstrapServers(targetBootstrapServers);
        }
        if (targetApiKey != null) {
            config.setTargetApiKey(targetApiKey);
        }
        if (targetApiSecret != null) {
            config.setTargetApiSecret(targetApiSecret);
        }
        if (outputTopic != null) {
            config.setTargetTopic(outputTopic);
        }

        // Processing Mode
        if (processingMode != null) {
            config.setProcessingMode(processingMode);
        }
        if (startTimestamp != null) {
            config.setStartTimestamp(startTimestamp);
        }
        if (endTimestamp != null) {
            config.setEndTimestamp(endTimestamp);
        }
        if (stopAtCurrent) {
            config.setStopAtCurrent(true);
        }

        // Consumer
        if (consumerGroup != null) {
            config.setConsumerGroupId(consumerGroup);
        }

        // Filtering
        if (includeMethods != null) {
            config.setIncludeMethods(new HashSet<>(Arrays.asList(includeMethods.split(","))));
        }
        if (includeConfigChanges) {
            config.setIncludeConfigChanges(true);
        }
        if (filterSubjects != null) {
            config.setSubjectFilters(new HashSet<>(Arrays.asList(filterSubjects.split(","))));
        }

        // Deduplication
        config.setEnableDeduplication(enableDeduplication);
        if (stateFile != null) {
            config.setStateFilePath(stateFile);
        }

        // Security
        if (securityProtocol != null) {
            config.setSecurityProtocol(securityProtocol);
        }
        if (saslMechanism != null) {
            config.setSaslMechanism(saslMechanism);
        }

        // Health server
        if (healthPort != null) {
            config.setHealthPort(healthPort);
        }

        // Processing threads
        if (processingThreads != null) {
            config.setProcessingThreads(processingThreads);
        }

        // Operational
        if (dryRun) {
            config.setDryRun(true);
        }
        if (pollTimeout != null) {
            config.setPollTimeoutMs(pollTimeout);
        }
        if (batchSize != null) {
            config.setBatchSize(batchSize);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SchemaChangeNotifierCommand()).execute(args);
        System.exit(exitCode);
    }
}

