package io.confluent.schemachange.processor;

import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.config.EnvironmentConfig;
import io.confluent.schemachange.model.AuditLogEvent;
import io.confluent.schemachange.model.SchemaChangeNotification;
import io.confluent.schemachange.registry.SchemaRegistryClient;
import io.confluent.schemachange.registry.SchemaRegistryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SchemaChangeProcessor.
 */
class SchemaChangeProcessorTest {

    private static final String TEST_ENV_ID = "env-test123";
    private static final String TEST_SOURCE = "crn://confluent.cloud/organization=org1/environment=" + TEST_ENV_ID + "/schema-registry=lsrc-xyz";

    private AppConfig config;
    private SchemaChangeProcessor processor;
    private StubSchemaRegistryClient stubClient;

    @BeforeEach
    void setUp() {
        config = new AppConfig();
        config.setIncludeMethods(Set.of(
                "schema-registry.RegisterSchema",
                "schema-registry.DeleteSchema",
                "schema-registry.DeleteSubject"
        ));
        // Add a test environment configuration
        config.addEnvironment(new EnvironmentConfig(
                TEST_ENV_ID,
                "https://test-sr.confluent.cloud",
                "test-key",
                "test-secret"
        ));
        // Use stub for schema registry client
        stubClient = new StubSchemaRegistryClient();
        processor = new SchemaChangeProcessor(config, stubClient);
    }

    @Test
    @DisplayName("Valid Schema Registry event should be relevant")
    void testIsRelevantEvent_ValidSchemaRegistryEvent() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );

        assertTrue(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Wrong event type should not be relevant")
    void testIsRelevantEvent_WrongEventType() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.kafka.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );

        assertFalse(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Wrong method name should not be relevant")
    void testIsRelevantEvent_WrongMethodName() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.GetSchema",
                "SUCCESS"
        );

        assertFalse(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Failed operation should not be relevant")
    void testIsRelevantEvent_FailedOperation() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "FAILURE"
        );

        assertFalse(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Unmonitored environment should not be relevant")
    void testIsRelevantEvent_UnmonitoredEnvironment() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );
        // Use a different environment that's not configured - update both source AND resourceName
        event.setSource("crn://confluent.cloud/organization=org1/environment=env-other/schema-registry=lsrc-xyz");
        event.getData().setResourceName("crn://confluent.cloud/organization=org1/environment=env-other/schema-registry=lsrc-xyz/subject/test");

        assertFalse(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Event matching subject filter should be relevant")
    void testIsRelevantEvent_WithSubjectFilter_Matching() {
        config.setSubjectFilters(Set.of("orders-*"));
        processor = new SchemaChangeProcessor(config, stubClient);

        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );
        // Set subject through request.data.subject
        AuditLogEvent.RequestData request = new AuditLogEvent.RequestData();
        AuditLogEvent.RequestInnerData requestData = new AuditLogEvent.RequestInnerData();
        requestData.setSubject("orders-value");
        request.setData(requestData);
        event.getData().setRequest(request);

        assertTrue(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("Event not matching subject filter should not be relevant")
    void testIsRelevantEvent_WithSubjectFilter_NotMatching() {
        config.setSubjectFilters(Set.of("orders-*"));
        processor = new SchemaChangeProcessor(config, stubClient);

        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );
        // Set subject through request.data.subject
        AuditLogEvent.RequestData request = new AuditLogEvent.RequestData();
        AuditLogEvent.RequestInnerData requestData = new AuditLogEvent.RequestInnerData();
        requestData.setSubject("payments-value");
        request.setData(requestData);
        event.getData().setRequest(request);

        assertFalse(processor.isRelevantEvent(event));
    }

    @Test
    @DisplayName("RegisterSchema event should create SCHEMA_REGISTERED notification")
    void testProcess_RegisterSchema() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );
        event.setTime("2024-01-15T10:30:00Z");

        // Set up result with schema ID
        AuditLogEvent.ResultData result = event.getData().getResult();
        AuditLogEvent.ResultInnerData resultData = new AuditLogEvent.ResultInnerData();
        resultData.setId(100001.0);
        result.setData(resultData);

        // Update request with full data
        AuditLogEvent.RequestInnerData requestData = event.getData().getRequest().getData();
        requestData.setSubject("orders-value");
        requestData.setSchema("{\"type\":\"record\",\"name\":\"Order\"}");
        requestData.setSchemaType("AVRO");

        // Configure stub to return schema info
        stubClient.addSchema(TEST_ENV_ID, 100001, new SchemaRegistryService.SchemaInfo(
                TEST_ENV_ID,
                100001,
                "orders-value",
                1,
                "{\"type\":\"record\",\"name\":\"Order\"}",
                "AVRO",
                null
        ));

        Optional<SchemaChangeNotification> result2 = processor.process(event);

        assertTrue(result2.isPresent());
        SchemaChangeNotification notification = result2.get();
        assertEquals(SchemaChangeNotification.EventType.SCHEMA_REGISTERED, notification.getEventType());
        assertEquals(100001, notification.getSchemaId());
        assertEquals("orders-value", notification.getSubject());
        assertEquals("AVRO", notification.getSchemaType());
        assertEquals(TEST_ENV_ID, notification.getEnvironmentId());
        assertNotNull(notification.getDataContractRegistered());
    }

    @Test
    @DisplayName("DeleteSchema event should create SCHEMA_DELETED notification")
    void testProcess_DeleteSchema() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.DeleteSchema",
                "SUCCESS"
        );
        event.setTime("2024-01-15T10:30:00Z");
        event.getData().getRequest().setVersion(3);
        // resourceName keeps the CRN format which the processor uses for subject on delete

        Optional<SchemaChangeNotification> result = processor.process(event);

        assertTrue(result.isPresent());
        SchemaChangeNotification notification = result.get();
        assertEquals(SchemaChangeNotification.EventType.SCHEMA_DELETED, notification.getEventType());
        // Subject is the CRN resourceName for delete operations
        assertNotNull(notification.getSubject());
        assertEquals(3, notification.getVersion());
        assertEquals(TEST_ENV_ID, notification.getEnvironmentId());
        assertNotNull(notification.getDataContractDeleted());
    }

    @Test
    @DisplayName("DeleteSubject event should create SUBJECT_DELETED notification")
    void testProcess_DeleteSubject() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.DeleteSubject",
                "SUCCESS"
        );
        event.setTime("2024-01-15T10:30:00Z");
        // resourceName keeps the CRN format which the processor uses for subject on delete

        Optional<SchemaChangeNotification> result = processor.process(event);

        assertTrue(result.isPresent());
        SchemaChangeNotification notification = result.get();
        assertEquals(SchemaChangeNotification.EventType.SUBJECT_DELETED, notification.getEventType());
        // Subject is the CRN resourceName for delete operations
        assertNotNull(notification.getSubject());
        assertEquals(TEST_ENV_ID, notification.getEnvironmentId());
        assertNotNull(notification.getSubjectDeleted());
    }

    @Test
    @DisplayName("Irrelevant event should return empty Optional")
    void testProcess_IrrelevantEvent() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.kafka.server/request",
                "kafka.CreateTopics",
                "SUCCESS"
        );

        Optional<SchemaChangeNotification> result = processor.process(event);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("Deduplication key should include subject, method, and schemaId")
    void testCreateDeduplicationKey() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );
        event.setTime("2024-01-15T10:30:00Z");

        // Override subject for this specific test
        event.getData().getRequest().getData().setSubject("orders-value");

        // Set up result with schema ID
        AuditLogEvent.ResultData result = event.getData().getResult();
        AuditLogEvent.ResultInnerData resultData = new AuditLogEvent.ResultInnerData();
        resultData.setId(100001.0);
        result.setData(resultData);

        String key = processor.createDeduplicationKey(event);

        // Key format is: subject:methodName:schemaId
        assertEquals("orders-value:schema-registry.RegisterSchema:100001", key);
    }

    @Test
    @DisplayName("extractEnvironmentId should extract from CRN")
    void testExtractEnvironmentId() {
        AuditLogEvent event = createAuditLogEvent(
                "io.confluent.sg.server/request",
                "schema-registry.RegisterSchema",
                "SUCCESS"
        );

        String envId = processor.extractEnvironmentId(event);

        assertEquals(TEST_ENV_ID, envId);
    }

    private AuditLogEvent createAuditLogEvent(String type, String methodName, String status) {
        AuditLogEvent event = new AuditLogEvent();
        event.setId("test-event-id");
        event.setType(type);
        event.setSource(TEST_SOURCE);

        AuditLogEvent.AuditLogData data = new AuditLogEvent.AuditLogData();
        data.setMethodName(methodName);
        data.setServiceName("SchemaRegistry");
        // Use resourceName with environment ID for proper extraction
        data.setResourceName("crn://confluent.cloud/organization=org1/environment=" + TEST_ENV_ID + "/schema-registry=lsrc-xyz/subject/test-subject");

        AuditLogEvent.ResultData result = new AuditLogEvent.ResultData();
        result.setStatus(status);
        data.setResult(result);

        // Add request with subject for proper subject extraction
        AuditLogEvent.RequestData request = new AuditLogEvent.RequestData();
        AuditLogEvent.RequestInnerData requestData = new AuditLogEvent.RequestInnerData();
        requestData.setSubject("test-subject");
        request.setData(requestData);
        data.setRequest(request);

        event.setData(data);
        return event;
    }

    /**
     * Stub implementation of SchemaRegistryClient for testing.
     */
    private static class StubSchemaRegistryClient implements SchemaRegistryClient {
        private final Map<String, SchemaRegistryService.SchemaInfo> schemas = new HashMap<>();

        public void addSchema(String envId, int schemaId, SchemaRegistryService.SchemaInfo info) {
            schemas.put(envId + ":" + schemaId, info);
        }

        @Override
        public boolean hasEnvironment(@Nonnull String environmentId) {
            return true;
        }

        @Nullable
        @Override
        public SchemaRegistryService.SchemaInfo getSchemaById(@Nonnull String environmentId, int schemaId) {
            return schemas.get(environmentId + ":" + schemaId);
        }

        @Nullable
        @Override
        public SchemaRegistryService.SchemaInfo getSchemaBySubjectVersion(@Nonnull String environmentId, @Nonnull String subject, int version) {
            return null;
        }

        @Override
        public void clearCache() {
            schemas.clear();
        }

        @Override
        public int getCacheSize() {
            return schemas.size();
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
