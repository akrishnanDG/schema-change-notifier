package io.confluent.schemachange.exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for custom exceptions.
 */
class ExceptionTest {

    // ==================== SchemaRegistryException Tests ====================

    @Test
    @DisplayName("SchemaRegistryException should store message")
    void testSchemaRegistryException_Message() {
        SchemaRegistryException ex = new SchemaRegistryException("Test message");

        assertEquals("Test message", ex.getMessage());
        assertNull(ex.getEnvironmentId());
        assertNull(ex.getSchemaId());
        assertEquals(-1, ex.getStatusCode());
    }

    @Test
    @DisplayName("SchemaRegistryException should store message and cause")
    void testSchemaRegistryException_MessageAndCause() {
        Exception cause = new RuntimeException("Root cause");
        SchemaRegistryException ex = new SchemaRegistryException("Test message", cause);

        assertEquals("Test message", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }

    @Test
    @DisplayName("SchemaRegistryException should store full context")
    void testSchemaRegistryException_FullContext() {
        SchemaRegistryException ex = new SchemaRegistryException(
                "Schema not found",
                "env-abc123",
                100001,
                404
        );

        assertEquals("Schema not found", ex.getMessage());
        assertEquals("env-abc123", ex.getEnvironmentId());
        assertEquals(100001, ex.getSchemaId());
        assertEquals(404, ex.getStatusCode());
    }

    @Test
    @DisplayName("SchemaRegistryException should store context with cause")
    void testSchemaRegistryException_ContextWithCause() {
        Exception cause = new RuntimeException("Network error");
        SchemaRegistryException ex = new SchemaRegistryException(
                "Failed to fetch schema",
                "env-xyz",
                100002,
                cause
        );

        assertEquals("Failed to fetch schema", ex.getMessage());
        assertEquals("env-xyz", ex.getEnvironmentId());
        assertEquals(100002, ex.getSchemaId());
        assertEquals(cause, ex.getCause());
        assertEquals(-1, ex.getStatusCode());
    }

    // ==================== ConfigurationException Tests ====================

    @Test
    @DisplayName("ConfigurationException should store message")
    void testConfigurationException_Message() {
        ConfigurationException ex = new ConfigurationException("Invalid config");

        assertEquals("Invalid config", ex.getMessage());
        assertNull(ex.getPropertyName());
    }

    @Test
    @DisplayName("ConfigurationException should store property name")
    void testConfigurationException_PropertyName() {
        ConfigurationException ex = new ConfigurationException(
                "Property is required",
                "bootstrap.servers"
        );

        assertEquals("Property is required", ex.getMessage());
        assertEquals("bootstrap.servers", ex.getPropertyName());
    }

    @Test
    @DisplayName("ConfigurationException should store cause")
    void testConfigurationException_Cause() {
        Exception cause = new IllegalArgumentException("Bad value");
        ConfigurationException ex = new ConfigurationException("Parse error", cause);

        assertEquals("Parse error", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }

    // ==================== NotificationProducerException Tests ====================

    @Test
    @DisplayName("NotificationProducerException should store message")
    void testNotificationProducerException_Message() {
        NotificationProducerException ex = new NotificationProducerException("Send failed");

        assertEquals("Send failed", ex.getMessage());
        assertNull(ex.getSubject());
    }

    @Test
    @DisplayName("NotificationProducerException should store message and cause")
    void testNotificationProducerException_MessageAndCause() {
        Exception cause = new RuntimeException("Kafka error");
        NotificationProducerException ex = new NotificationProducerException("Send failed", cause);

        assertEquals("Send failed", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }

    @Test
    @DisplayName("NotificationProducerException should store subject and cause")
    void testNotificationProducerException_SubjectAndCause() {
        Exception cause = new RuntimeException("Serialization failed");
        NotificationProducerException ex = new NotificationProducerException(
                "Failed to produce notification",
                "orders-value",
                cause
        );

        assertEquals("Failed to produce notification", ex.getMessage());
        assertEquals("orders-value", ex.getSubject());
        assertEquals(cause, ex.getCause());
    }

    // ==================== Exception Hierarchy Tests ====================

    @Test
    @DisplayName("All custom exceptions should extend RuntimeException")
    void testExceptionHierarchy() {
        assertTrue(RuntimeException.class.isAssignableFrom(SchemaRegistryException.class));
        assertTrue(RuntimeException.class.isAssignableFrom(ConfigurationException.class));
        assertTrue(RuntimeException.class.isAssignableFrom(NotificationProducerException.class));
    }

    @Test
    @DisplayName("Exceptions should be catchable as RuntimeException")
    void testCatchAsRuntimeException() {
        try {
            throw new SchemaRegistryException("Test");
        } catch (RuntimeException e) {
            assertInstanceOf(SchemaRegistryException.class, e);
        }

        try {
            throw new ConfigurationException("Test");
        } catch (RuntimeException e) {
            assertInstanceOf(ConfigurationException.class, e);
        }

        try {
            throw new NotificationProducerException("Test");
        } catch (RuntimeException e) {
            assertInstanceOf(NotificationProducerException.class, e);
        }
    }
}

