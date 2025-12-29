package io.confluent.schemachange.config;

import io.confluent.schemachange.exception.ConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AppConfig validation and configuration management.
 */
class AppConfigTest {

    private AppConfig config;

    @BeforeEach
    void setUp() {
        config = createValidConfig();
    }

    private AppConfig createValidConfig() {
        AppConfig cfg = new AppConfig();
        cfg.setAuditLogBootstrapServers("pkc-test.confluent.cloud:9092");
        cfg.setAuditLogApiKey("audit-key");
        cfg.setAuditLogApiSecret("audit-secret");
        cfg.setTargetBootstrapServers("pkc-target.confluent.cloud:9092");
        cfg.setTargetApiKey("target-key");
        cfg.setTargetApiSecret("target-secret");
        cfg.setTargetTopic("schema-changes");
        cfg.addEnvironment(new EnvironmentConfig(
                "env-test123",
                "https://psrc-test.confluent.cloud",
                "sr-key",
                "sr-secret"
        ));
        return cfg;
    }

    @Test
    @DisplayName("Valid configuration should pass validation")
    void testValidate_ValidConfig() {
        assertDoesNotThrow(() -> config.validate());
    }

    @Test
    @DisplayName("Missing audit log bootstrap servers should fail validation")
    void testValidate_MissingAuditLogBootstrapServers() {
        config.setAuditLogBootstrapServers(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("audit.log.bootstrap.servers"));
    }

    @Test
    @DisplayName("Empty audit log bootstrap servers should fail validation")
    void testValidate_EmptyAuditLogBootstrapServers() {
        config.setAuditLogBootstrapServers("   ");

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("audit.log.bootstrap.servers"));
    }

    @Test
    @DisplayName("Missing audit log API key should fail validation")
    void testValidate_MissingAuditLogApiKey() {
        config.setAuditLogApiKey(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("audit.log.api.key"));
    }

    @Test
    @DisplayName("Missing audit log API secret should fail validation")
    void testValidate_MissingAuditLogApiSecret() {
        config.setAuditLogApiSecret(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("audit.log.api.secret"));
    }

    @Test
    @DisplayName("Missing target bootstrap servers should fail validation")
    void testValidate_MissingTargetBootstrapServers() {
        config.setTargetBootstrapServers(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("target.bootstrap.servers"));
    }

    @Test
    @DisplayName("Missing target topic should fail validation")
    void testValidate_MissingTargetTopic() {
        config.setTargetTopic(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("target.topic"));
    }

    @Test
    @DisplayName("No environments configured should fail validation")
    void testValidate_NoEnvironments() {
        config.setEnvironments(new java.util.HashMap<>());

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("environment"));
    }

    @Test
    @DisplayName("TIMESTAMP mode without start timestamp should fail validation")
    void testValidate_TimestampModeWithoutStartTimestamp() {
        config.setProcessingMode(ProcessingMode.TIMESTAMP);
        config.setStartTimestamp(null);

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                () -> config.validate()
        );
        assertTrue(exception.getMessage().contains("start.timestamp"));
    }

    @Test
    @DisplayName("TIMESTAMP mode with start timestamp should pass validation")
    void testValidate_TimestampModeWithStartTimestamp() {
        config.setProcessingMode(ProcessingMode.TIMESTAMP);
        config.setStartTimestamp("2024-01-01T00:00:00Z");

        assertDoesNotThrow(() -> config.validate());
    }

    @Test
    @DisplayName("hasEnvironment should return true for configured environment")
    void testHasEnvironment_Configured() {
        assertTrue(config.hasEnvironment("env-test123"));
    }

    @Test
    @DisplayName("hasEnvironment should return false for unconfigured environment")
    void testHasEnvironment_NotConfigured() {
        assertFalse(config.hasEnvironment("env-other"));
    }

    @Test
    @DisplayName("getMonitoredEnvironmentIds should return all environment IDs")
    void testGetMonitoredEnvironmentIds() {
        config.addEnvironment(new EnvironmentConfig(
                "env-second",
                "https://psrc-second.confluent.cloud",
                "sr-key2",
                "sr-secret2"
        ));

        Set<String> envIds = config.getMonitoredEnvironmentIds();

        assertEquals(2, envIds.size());
        assertTrue(envIds.contains("env-test123"));
        assertTrue(envIds.contains("env-second"));
    }

    @Test
    @DisplayName("Default values should be set correctly")
    void testDefaultValues() {
        AppConfig defaultConfig = new AppConfig();

        assertEquals("confluent-audit-log-events", defaultConfig.getAuditLogTopic());
        assertEquals("schema-change-notifier", defaultConfig.getConsumerGroupId());
        assertEquals(ProcessingMode.STREAM, defaultConfig.getProcessingMode());
        assertTrue(defaultConfig.isOnlySuccessful());
        assertFalse(defaultConfig.isDryRun());
        assertEquals(1000, defaultConfig.getPollTimeoutMs());
        assertEquals(100, defaultConfig.getBatchSize());
    }

    @Test
    @DisplayName("setIncludeConfigChanges should add config methods")
    void testSetIncludeConfigChanges() {
        config.setIncludeConfigChanges(true);

        assertTrue(config.getIncludeMethods().contains("schema-registry.UpdateCompatibility"));
        assertTrue(config.getIncludeMethods().contains("schema-registry.UpdateMode"));
    }

    @Test
    @DisplayName("hasSubjectFilters should return false when no filters set")
    void testHasSubjectFilters_Empty() {
        assertFalse(config.hasSubjectFilters());
    }

    @Test
    @DisplayName("hasSubjectFilters should return true when filters set")
    void testHasSubjectFilters_WithFilters() {
        config.setSubjectFilters(Set.of("orders-*", "payments-*"));

        assertTrue(config.hasSubjectFilters());
    }
}

