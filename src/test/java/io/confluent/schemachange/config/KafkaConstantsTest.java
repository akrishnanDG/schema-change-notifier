package io.confluent.schemachange.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static io.confluent.schemachange.config.KafkaConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaConstants.
 */
class KafkaConstantsTest {

    @Test
    @DisplayName("Security constants should have expected values")
    void testSecurityConstants() {
        assertEquals("SASL_SSL", SECURITY_PROTOCOL);
        assertEquals("PLAIN", SASL_MECHANISM);
        assertTrue(JAAS_CONFIG_TEMPLATE.contains("PlainLoginModule"));
        assertTrue(JAAS_CONFIG_TEMPLATE.contains("%s"));
    }

    @Test
    @DisplayName("Schema Registry constants should have expected values")
    void testSchemaRegistryConstants() {
        assertEquals("application/vnd.schemaregistry.v1+json", SCHEMA_REGISTRY_CONTENT_TYPE);
        assertEquals("USER_INFO", BASIC_AUTH_CREDENTIALS_SOURCE);
        assertEquals("AVRO", DEFAULT_SCHEMA_TYPE);
    }

    @Test
    @DisplayName("Audit log constants should have expected values")
    void testAuditLogConstants() {
        assertEquals("confluent-audit-log-events", AUDIT_LOG_TOPIC);
        assertEquals("io.confluent.sg.server/request", SCHEMA_REGISTRY_EVENT_TYPE);
    }

    @Test
    @DisplayName("Consumer constants should have reasonable defaults")
    void testConsumerConstants() {
        assertEquals("schema-change-notifier", DEFAULT_CONSUMER_GROUP);
        assertEquals(1000, DEFAULT_POLL_TIMEOUT_MS);
        assertEquals(100, DEFAULT_BATCH_SIZE);
    }

    @Test
    @DisplayName("Producer constants should have expected values")
    void testProducerConstants() {
        assertEquals("all", ACKS_ALL);
        assertEquals("snappy", COMPRESSION_TYPE);
        assertEquals(16384, PRODUCER_BATCH_SIZE);
        assertEquals(10, PRODUCER_LINGER_MS);
        assertEquals(3, DEFAULT_RETRIES);
        assertEquals(1000, DEFAULT_RETRY_BACKOFF_MS);
    }

    @Test
    @DisplayName("Timeout constants should be positive")
    void testTimeoutConstants() {
        assertTrue(HTTP_CONNECT_TIMEOUT_SECONDS > 0);
        assertTrue(HTTP_READ_TIMEOUT_SECONDS > 0);
        assertTrue(PRODUCER_SEND_TIMEOUT_SECONDS > 0);
        assertTrue(CLOSE_TIMEOUT_SECONDS > 0);
    }

    @Test
    @DisplayName("Deduplication constants should have reasonable values")
    void testDeduplicationConstants() {
        assertTrue(MAX_DEDUP_EVENTS > 0);
        assertNotNull(DEFAULT_STATE_FILE_PATH);
        assertFalse(DEFAULT_STATE_FILE_PATH.isEmpty());
    }

    @Test
    @DisplayName("Result status constant should be correct")
    void testResultStatusConstant() {
        assertEquals("SUCCESS", STATUS_SUCCESS);
    }

    @Test
    @DisplayName("JAAS config template should format correctly")
    void testJaasConfigTemplate() {
        String result = String.format(JAAS_CONFIG_TEMPLATE, "myuser", "mypassword");

        assertTrue(result.contains("username='myuser'"));
        assertTrue(result.contains("password='mypassword'"));
        assertTrue(result.endsWith(";"));
    }

    @Test
    @DisplayName("KafkaConstants should not be instantiable")
    void testNotInstantiable() throws Exception {
        Constructor<KafkaConstants> constructor = KafkaConstants.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        
        constructor.setAccessible(true);
        // Should be able to call it but it's marked private to prevent instantiation
        assertNotNull(constructor.newInstance());
    }

    @Test
    @DisplayName("All constants should be final")
    void testConstantsAreFinal() {
        var fields = KafkaConstants.class.getDeclaredFields();
        
        for (var field : fields) {
            if (!field.isSynthetic()) {
                assertTrue(Modifier.isFinal(field.getModifiers()),
                        "Field " + field.getName() + " should be final");
                assertTrue(Modifier.isStatic(field.getModifiers()),
                        "Field " + field.getName() + " should be static");
                assertTrue(Modifier.isPublic(field.getModifiers()),
                        "Field " + field.getName() + " should be public");
            }
        }
    }
}

