package io.confluent.schemachange.util;

import io.confluent.schemachange.model.AuditLogEvent;
import io.confluent.schemachange.model.SchemaChangeNotification;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void testFromJson_ValidAuditLogEvent() {
        String json = """
                {
                    "id": "event-123",
                    "specversion": "1.0",
                    "type": "io.confluent.sg.server/request",
                    "source": "crn://confluent.cloud/...",
                    "time": "2024-01-15T10:30:00Z",
                    "data": {
                        "serviceName": "SchemaRegistry",
                        "methodName": "schema-registry.RegisterSchema",
                        "resourceName": "orders-value",
                        "result": {
                            "status": "SUCCESS"
                        },
                        "response": {
                            "id": 100001,
                            "version": 3
                        }
                    }
                }
                """;

        AuditLogEvent event = JsonUtils.fromJson(json, AuditLogEvent.class);

        assertNotNull(event);
        assertEquals("event-123", event.getId());
        assertEquals("io.confluent.sg.server/request", event.getType());
        assertEquals("2024-01-15T10:30:00Z", event.getTime());
        assertNotNull(event.getData());
        assertEquals("SchemaRegistry", event.getData().getServiceName());
        assertEquals("schema-registry.RegisterSchema", event.getData().getMethodName());
        assertEquals("orders-value", event.getData().getResourceName());
        assertEquals("SUCCESS", event.getData().getResult().getStatus());
        assertEquals(100001, event.getData().getResponse().getId());
        assertEquals(3, event.getData().getResponse().getVersion());
    }

    @Test
    void testFromJson_InvalidJson() {
        String invalidJson = "not valid json";

        AuditLogEvent event = JsonUtils.fromJson(invalidJson, AuditLogEvent.class);

        assertNull(event);
    }

    @Test
    void testFromJson_NullInput() {
        AuditLogEvent event = JsonUtils.fromJson(null, AuditLogEvent.class);

        assertNull(event);
    }

    @Test
    void testFromJson_EmptyInput() {
        AuditLogEvent event = JsonUtils.fromJson("", AuditLogEvent.class);

        assertNull(event);
    }

    @Test
    void testToJson_SchemaChangeNotification() {
        SchemaChangeNotification notification = SchemaChangeNotification.builder()
                .eventType(SchemaChangeNotification.EventType.SCHEMA_REGISTERED)
                .schemaId(100001)
                .subject("orders-value")
                .version(3)
                .schemaType("AVRO")
                .timestamp("2024-01-15T10:30:00Z")
                .dataContractRegistered(new SchemaChangeNotification.DataContractRegistered(
                        "{\"type\":\"record\",\"name\":\"Order\"}",
                        null
                ))
                .build();

        String json = JsonUtils.toJson(notification);

        assertNotNull(json);
        assertTrue(json.contains("\"event_type\":\"SCHEMA_REGISTERED\""));
        assertTrue(json.contains("\"schema_id\":100001"));
        assertTrue(json.contains("\"subject\":\"orders-value\""));
        assertTrue(json.contains("\"version\":3"));
        assertTrue(json.contains("\"schema_type\":\"AVRO\""));
        assertTrue(json.contains("\"data_contract_registered\""));
    }

    @Test
    void testToJson_NullInput() {
        String json = JsonUtils.toJson(null);

        assertNull(json);
    }

    @Test
    void testToPrettyJson() {
        SchemaChangeNotification notification = SchemaChangeNotification.builder()
                .eventType(SchemaChangeNotification.EventType.SCHEMA_REGISTERED)
                .schemaId(100001)
                .subject("orders-value")
                .build();

        String json = JsonUtils.toPrettyJson(notification);

        assertNotNull(json);
        assertTrue(json.contains("\n")); // Pretty printed should have newlines
    }

    @Test
    void testFromJson_UnknownProperties() {
        // Should ignore unknown properties
        String json = """
                {
                    "id": "event-123",
                    "type": "io.confluent.sg.server/request",
                    "unknownField": "should be ignored",
                    "data": {
                        "methodName": "schema-registry.RegisterSchema",
                        "anotherUnknown": 123
                    }
                }
                """;

        AuditLogEvent event = JsonUtils.fromJson(json, AuditLogEvent.class);

        assertNotNull(event);
        assertEquals("event-123", event.getId());
        assertEquals("io.confluent.sg.server/request", event.getType());
        assertEquals("schema-registry.RegisterSchema", event.getData().getMethodName());
    }
}

