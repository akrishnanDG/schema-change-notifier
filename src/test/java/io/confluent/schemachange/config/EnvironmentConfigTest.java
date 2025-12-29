package io.confluent.schemachange.config;

import io.confluent.schemachange.exception.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EnvironmentConfig validation.
 */
class EnvironmentConfigTest {

    @Test
    @DisplayName("Valid environment config should pass validation")
    void testValidate_ValidConfig() {
        EnvironmentConfig config = new EnvironmentConfig(
                "env-abc123",
                "https://psrc-test.confluent.cloud",
                "api-key",
                "api-secret"
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    @DisplayName("Missing environment ID should fail validation")
    void testValidate_MissingEnvironmentId() {
        EnvironmentConfig config = new EnvironmentConfig(
                null,
                "https://psrc-test.confluent.cloud",
                "api-key",
                "api-secret"
        );

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                config::validate
        );
        assertTrue(exception.getMessage().contains("Environment ID"));
    }

    @Test
    @DisplayName("Empty environment ID should fail validation")
    void testValidate_EmptyEnvironmentId() {
        EnvironmentConfig config = new EnvironmentConfig(
                "  ",
                "https://psrc-test.confluent.cloud",
                "api-key",
                "api-secret"
        );

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                config::validate
        );
        assertTrue(exception.getMessage().contains("Environment ID"));
    }

    @Test
    @DisplayName("Missing Schema Registry URL should fail validation")
    void testValidate_MissingSchemaRegistryUrl() {
        EnvironmentConfig config = new EnvironmentConfig(
                "env-abc123",
                null,
                "api-key",
                "api-secret"
        );

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                config::validate
        );
        assertTrue(exception.getMessage().contains("Schema Registry URL"));
    }

    @Test
    @DisplayName("Missing API key should fail validation")
    void testValidate_MissingApiKey() {
        EnvironmentConfig config = new EnvironmentConfig(
                "env-abc123",
                "https://psrc-test.confluent.cloud",
                null,
                "api-secret"
        );

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                config::validate
        );
        assertTrue(exception.getMessage().contains("API key"));
    }

    @Test
    @DisplayName("Missing API secret should fail validation")
    void testValidate_MissingApiSecret() {
        EnvironmentConfig config = new EnvironmentConfig(
                "env-abc123",
                "https://psrc-test.confluent.cloud",
                "api-key",
                null
        );

        ConfigurationException exception = assertThrows(
                ConfigurationException.class,
                config::validate
        );
        assertTrue(exception.getMessage().contains("API secret"));
    }

    @Test
    @DisplayName("Default constructor should create empty config")
    void testDefaultConstructor() {
        EnvironmentConfig config = new EnvironmentConfig();

        assertNull(config.getEnvironmentId());
        assertNull(config.getSchemaRegistryUrl());
        assertNull(config.getSchemaRegistryApiKey());
        assertNull(config.getSchemaRegistryApiSecret());
    }

    @Test
    @DisplayName("Setters should update values")
    void testSetters() {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setEnvironmentId("env-xyz");
        config.setSchemaRegistryUrl("https://sr.test.com");
        config.setSchemaRegistryApiKey("my-key");
        config.setSchemaRegistryApiSecret("my-secret");

        assertEquals("env-xyz", config.getEnvironmentId());
        assertEquals("https://sr.test.com", config.getSchemaRegistryUrl());
        assertEquals("my-key", config.getSchemaRegistryApiKey());
        assertEquals("my-secret", config.getSchemaRegistryApiSecret());
    }

    @Test
    @DisplayName("toString should not expose secrets")
    void testToString() {
        EnvironmentConfig config = new EnvironmentConfig(
                "env-abc123",
                "https://psrc-test.confluent.cloud",
                "api-key",
                "api-secret"
        );

        String result = config.toString();

        assertTrue(result.contains("env-abc123"));
        assertTrue(result.contains("psrc-test.confluent.cloud"));
        assertFalse(result.contains("api-key"));
        assertFalse(result.contains("api-secret"));
    }
}

