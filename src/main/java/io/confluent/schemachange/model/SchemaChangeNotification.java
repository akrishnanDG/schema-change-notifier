package io.confluent.schemachange.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a schema change notification to be produced to the target topic.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaChangeNotification {

    @JsonProperty("event_type")
    private EventType eventType;

    @JsonProperty("schema_id")
    private Integer schemaId;

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("version")
    private Integer version;

    @JsonProperty("schema_type")
    private String schemaType;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("data_contract_registered")
    private DataContractRegistered dataContractRegistered;

    @JsonProperty("data_contract_deleted")
    private DataContractDeleted dataContractDeleted;

    @JsonProperty("subject_deleted")
    private SubjectDeleted subjectDeleted;

    @JsonProperty("compatibility_updated")
    private CompatibilityUpdated compatibilityUpdated;

    @JsonProperty("mode_updated")
    private ModeUpdated modeUpdated;

    @JsonProperty("audit_log_event_id")
    private String auditLogEventId;

    @JsonProperty("environment_id")
    private String environmentId;

    // Builder pattern for fluent construction

    public static Builder builder() {
        return new Builder();
    }

    // Getters and Setters

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Integer getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Integer schemaId) {
        this.schemaId = schemaId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public DataContractRegistered getDataContractRegistered() {
        return dataContractRegistered;
    }

    public void setDataContractRegistered(DataContractRegistered dataContractRegistered) {
        this.dataContractRegistered = dataContractRegistered;
    }

    public DataContractDeleted getDataContractDeleted() {
        return dataContractDeleted;
    }

    public void setDataContractDeleted(DataContractDeleted dataContractDeleted) {
        this.dataContractDeleted = dataContractDeleted;
    }

    public SubjectDeleted getSubjectDeleted() {
        return subjectDeleted;
    }

    public void setSubjectDeleted(SubjectDeleted subjectDeleted) {
        this.subjectDeleted = subjectDeleted;
    }

    public CompatibilityUpdated getCompatibilityUpdated() {
        return compatibilityUpdated;
    }

    public void setCompatibilityUpdated(CompatibilityUpdated compatibilityUpdated) {
        this.compatibilityUpdated = compatibilityUpdated;
    }

    public ModeUpdated getModeUpdated() {
        return modeUpdated;
    }

    public void setModeUpdated(ModeUpdated modeUpdated) {
        this.modeUpdated = modeUpdated;
    }

    public String getAuditLogEventId() {
        return auditLogEventId;
    }

    public void setAuditLogEventId(String auditLogEventId) {
        this.auditLogEventId = auditLogEventId;
    }

    public String getEnvironmentId() {
        return environmentId;
    }

    public void setEnvironmentId(String environmentId) {
        this.environmentId = environmentId;
    }

    /**
     * Event types for schema changes.
     */
    public enum EventType {
        SCHEMA_REGISTERED,
        SCHEMA_DELETED,
        SUBJECT_DELETED,
        COMPATIBILITY_UPDATED,
        MODE_UPDATED
    }

    /**
     * Data contract registration details.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataContractRegistered {

        @JsonProperty("schema")
        private String schema;

        @JsonProperty("references")
        private Object references;

        public DataContractRegistered() {
        }

        public DataContractRegistered(String schema, Object references) {
            this.schema = schema;
            this.references = references;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public Object getReferences() {
            return references;
        }

        public void setReferences(Object references) {
            this.references = references;
        }
    }

    /**
     * Data contract deletion details.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataContractDeleted {

        @JsonProperty("permanent")
        private Boolean permanent;

        public DataContractDeleted() {
        }

        public DataContractDeleted(Boolean permanent) {
            this.permanent = permanent;
        }

        public Boolean getPermanent() {
            return permanent;
        }

        public void setPermanent(Boolean permanent) {
            this.permanent = permanent;
        }
    }

    /**
     * Subject deletion details.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SubjectDeleted {

        @JsonProperty("permanent")
        private Boolean permanent;

        @JsonProperty("versions_deleted")
        private Integer versionsDeleted;

        public SubjectDeleted() {
        }

        public SubjectDeleted(Boolean permanent, Integer versionsDeleted) {
            this.permanent = permanent;
            this.versionsDeleted = versionsDeleted;
        }

        public Boolean getPermanent() {
            return permanent;
        }

        public void setPermanent(Boolean permanent) {
            this.permanent = permanent;
        }

        public Integer getVersionsDeleted() {
            return versionsDeleted;
        }

        public void setVersionsDeleted(Integer versionsDeleted) {
            this.versionsDeleted = versionsDeleted;
        }
    }

    /**
     * Compatibility update details.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CompatibilityUpdated {

        @JsonProperty("new_compatibility")
        private String newCompatibility;

        public CompatibilityUpdated() {
        }

        public CompatibilityUpdated(String newCompatibility) {
            this.newCompatibility = newCompatibility;
        }

        public String getNewCompatibility() {
            return newCompatibility;
        }

        public void setNewCompatibility(String newCompatibility) {
            this.newCompatibility = newCompatibility;
        }
    }

    /**
     * Mode update details.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ModeUpdated {

        @JsonProperty("new_mode")
        private String newMode;

        public ModeUpdated() {
        }

        public ModeUpdated(String newMode) {
            this.newMode = newMode;
        }

        public String getNewMode() {
            return newMode;
        }

        public void setNewMode(String newMode) {
            this.newMode = newMode;
        }
    }

    /**
     * Builder for SchemaChangeNotification.
     */
    public static class Builder {
        private final SchemaChangeNotification notification = new SchemaChangeNotification();

        public Builder eventType(EventType eventType) {
            notification.setEventType(eventType);
            return this;
        }

        public Builder schemaId(Integer schemaId) {
            notification.setSchemaId(schemaId);
            return this;
        }

        public Builder subject(String subject) {
            notification.setSubject(subject);
            return this;
        }

        public Builder version(Integer version) {
            notification.setVersion(version);
            return this;
        }

        public Builder schemaType(String schemaType) {
            notification.setSchemaType(schemaType);
            return this;
        }

        public Builder timestamp(String timestamp) {
            notification.setTimestamp(timestamp);
            return this;
        }

        public Builder dataContractRegistered(DataContractRegistered data) {
            notification.setDataContractRegistered(data);
            return this;
        }

        public Builder dataContractDeleted(DataContractDeleted data) {
            notification.setDataContractDeleted(data);
            return this;
        }

        public Builder subjectDeleted(SubjectDeleted data) {
            notification.setSubjectDeleted(data);
            return this;
        }

        public Builder compatibilityUpdated(CompatibilityUpdated data) {
            notification.setCompatibilityUpdated(data);
            return this;
        }

        public Builder modeUpdated(ModeUpdated data) {
            notification.setModeUpdated(data);
            return this;
        }

        public Builder auditLogEventId(String eventId) {
            notification.setAuditLogEventId(eventId);
            return this;
        }

        public Builder environmentId(String environmentId) {
            notification.setEnvironmentId(environmentId);
            return this;
        }

        public SchemaChangeNotification build() {
            return notification;
        }
    }
}

