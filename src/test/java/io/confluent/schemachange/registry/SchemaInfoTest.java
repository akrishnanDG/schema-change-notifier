package io.confluent.schemachange.registry;

import io.confluent.schemachange.registry.SchemaRegistryService.SchemaInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SchemaInfo record.
 */
class SchemaInfoTest {

    @Test
    @DisplayName("SchemaInfo should store all fields correctly")
    void testSchemaInfoFields() {
        SchemaInfo info = new SchemaInfo(
                "env-abc123",
                100001,
                "orders-value",
                3,
                "{\"type\":\"record\",\"name\":\"Order\"}",
                "AVRO",
                null
        );

        assertEquals("env-abc123", info.environmentId());
        assertEquals(100001, info.schemaId());
        assertEquals("orders-value", info.subject());
        assertEquals(3, info.version());
        assertEquals("{\"type\":\"record\",\"name\":\"Order\"}", info.schema());
        assertEquals("AVRO", info.schemaType());
        assertNull(info.references());
    }

    @Test
    @DisplayName("SchemaInfo should default to AVRO schema type when null")
    void testSchemaInfoDefaultSchemaType() {
        SchemaInfo info = new SchemaInfo(
                "env-abc123",
                100001,
                "orders-value",
                1,
                "{}",
                null,  // null schema type
                null
        );

        assertEquals("AVRO", info.schemaType());
    }

    @Test
    @DisplayName("SchemaInfo should allow null values for optional fields")
    void testSchemaInfoNullableFields() {
        SchemaInfo info = new SchemaInfo(
                null,
                null,
                null,
                null,
                null,
                "AVRO",
                null
        );

        assertNull(info.environmentId());
        assertNull(info.schemaId());
        assertNull(info.subject());
        assertNull(info.version());
        assertNull(info.schema());
        assertNull(info.references());
    }

    @Test
    @DisplayName("SchemaInfo should support different schema types")
    void testSchemaInfoSchemaTypes() {
        SchemaInfo avroInfo = new SchemaInfo(null, null, null, null, null, "AVRO", null);
        SchemaInfo jsonInfo = new SchemaInfo(null, null, null, null, null, "JSON", null);
        SchemaInfo protobufInfo = new SchemaInfo(null, null, null, null, null, "PROTOBUF", null);

        assertEquals("AVRO", avroInfo.schemaType());
        assertEquals("JSON", jsonInfo.schemaType());
        assertEquals("PROTOBUF", protobufInfo.schemaType());
    }

    @Test
    @DisplayName("SchemaInfo equals should work correctly")
    void testSchemaInfoEquals() {
        SchemaInfo info1 = new SchemaInfo("env-1", 1001, "subject", 1, "{}", "AVRO", null);
        SchemaInfo info2 = new SchemaInfo("env-1", 1001, "subject", 1, "{}", "AVRO", null);
        SchemaInfo info3 = new SchemaInfo("env-2", 1001, "subject", 1, "{}", "AVRO", null);

        assertEquals(info1, info2);
        assertNotEquals(info1, info3);
    }

    @Test
    @DisplayName("SchemaInfo hashCode should be consistent with equals")
    void testSchemaInfoHashCode() {
        SchemaInfo info1 = new SchemaInfo("env-1", 1001, "subject", 1, "{}", "AVRO", null);
        SchemaInfo info2 = new SchemaInfo("env-1", 1001, "subject", 1, "{}", "AVRO", null);

        assertEquals(info1.hashCode(), info2.hashCode());
    }

    @Test
    @DisplayName("SchemaInfo toString should contain field values")
    void testSchemaInfoToString() {
        SchemaInfo info = new SchemaInfo("env-abc", 1001, "my-subject", 2, "{}", "JSON", null);

        String result = info.toString();

        assertTrue(result.contains("env-abc"));
        assertTrue(result.contains("1001"));
        assertTrue(result.contains("my-subject"));
        assertTrue(result.contains("JSON"));
    }

    @Test
    @DisplayName("SchemaInfo deprecated getters should return correct values")
    @SuppressWarnings("deprecation")
    void testSchemaInfoDeprecatedGetters() {
        SchemaInfo info = new SchemaInfo(
                "env-abc123",
                100001,
                "orders-value",
                3,
                "{\"type\":\"record\"}",
                "AVRO",
                "[]"
        );

        assertEquals("{\"type\":\"record\"}", info.getSchema());
        assertEquals("AVRO", info.getSchemaType());
        assertEquals(3, info.getVersion());
        assertEquals("[]", info.getReferences());
    }

    @Test
    @DisplayName("SchemaInfo should be immutable")
    void testSchemaInfoImmutability() {
        // Records are inherently immutable - this test verifies the type is a record
        assertTrue(SchemaInfo.class.isRecord());
    }
}

