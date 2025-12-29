package io.confluent.schemachange.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Confluent Cloud audit log event following the CloudEvents specification.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuditLogEvent {

    @JsonProperty("id")
    private String id;

    @JsonProperty("specversion")
    private String specVersion;

    @JsonProperty("type")
    private String type;

    @JsonProperty("source")
    private String source;

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("time")
    private String time;

    @JsonProperty("data")
    private AuditLogData data;

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSpecVersion() {
        return specVersion;
    }

    public void setSpecVersion(String specVersion) {
        this.specVersion = specVersion;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public AuditLogData getData() {
        return data;
    }

    public void setData(AuditLogData data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "AuditLogEvent{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", time='" + time + '\'' +
                ", data=" + data +
                '}';
    }

    /**
     * The data payload of the audit log event.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuditLogData {

        @JsonProperty("serviceName")
        private String serviceName;

        @JsonProperty("methodName")
        private String methodName;

        @JsonProperty("resourceName")
        private String resourceName;

        @JsonProperty("authenticationInfo")
        private AuthenticationInfo authenticationInfo;

        @JsonProperty("request")
        private RequestData request;

        @JsonProperty("response")
        private ResponseData response;

        @JsonProperty("result")
        private ResultData result;

        // Getters and Setters

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String resourceName) {
            this.resourceName = resourceName;
        }

        public AuthenticationInfo getAuthenticationInfo() {
            return authenticationInfo;
        }

        public void setAuthenticationInfo(AuthenticationInfo authenticationInfo) {
            this.authenticationInfo = authenticationInfo;
        }

        public RequestData getRequest() {
            return request;
        }

        public void setRequest(RequestData request) {
            this.request = request;
        }

        public ResponseData getResponse() {
            return response;
        }

        public void setResponse(ResponseData response) {
            this.response = response;
        }

        public ResultData getResult() {
            return result;
        }

        public void setResult(ResultData result) {
            this.result = result;
        }

        @Override
        public String toString() {
            return "AuditLogData{" +
                    "serviceName='" + serviceName + '\'' +
                    ", methodName='" + methodName + '\'' +
                    ", resourceName='" + resourceName + '\'' +
                    ", result=" + result +
                    '}';
        }
    }

    /**
     * Authentication information from the audit log.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuthenticationInfo {

        @JsonProperty("principal")
        private Object principal;

        public Object getPrincipal() {
            return principal;
        }

        public void setPrincipal(Object principal) {
            this.principal = principal;
        }
    }

    /**
     * Request data containing schema information.
     * The actual schema details are in the nested "data" object.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RequestData {

        @JsonProperty("accessType")
        private String accessType;

        @JsonProperty("data")
        private RequestInnerData data;

        // Direct fields (for some event types)
        @JsonProperty("subject")
        private String subject;

        @JsonProperty("version")
        private Integer version;

        @JsonProperty("compatibility")
        private String compatibility;

        @JsonProperty("mode")
        private String mode;

        // Getters and Setters

        public String getAccessType() {
            return accessType;
        }

        public void setAccessType(String accessType) {
            this.accessType = accessType;
        }

        public RequestInnerData getData() {
            return data;
        }

        public void setData(RequestInnerData data) {
            this.data = data;
        }

        // Convenience methods to get schema info from nested data
        public String getSchema() {
            return data != null ? data.getSchema() : null;
        }

        public String getSchemaType() {
            return data != null ? data.getSchemaType() : null;
        }

        public Object getReferences() {
            return data != null ? data.getReferences() : null;
        }

        public String getSubject() {
            // Try nested data first, then direct field
            if (data != null && data.getSubject() != null) {
                return data.getSubject();
            }
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

        public String getCompatibility() {
            return compatibility;
        }

        public void setCompatibility(String compatibility) {
            this.compatibility = compatibility;
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }
    }

    /**
     * Inner data object within request containing actual schema details.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RequestInnerData {

        @JsonProperty("subject")
        private String subject;

        @JsonProperty("schema")
        private String schema;

        @JsonProperty("schemaType")
        private String schemaType;

        @JsonProperty("references")
        private Object references;

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getSchemaType() {
            return schemaType;
        }

        public void setSchemaType(String schemaType) {
            this.schemaType = schemaType;
        }

        public Object getReferences() {
            return references;
        }

        public void setReferences(Object references) {
            this.references = references;
        }
    }

    /**
     * Response data containing schema ID (legacy format).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResponseData {

        @JsonProperty("id")
        private Integer id;

        @JsonProperty("version")
        private Integer version;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }
    }

    /**
     * Result data indicating success or failure.
     * Contains nested "data" with schema ID for RegisterSchema events.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResultData {

        @JsonProperty("status")
        private String status;

        @JsonProperty("message")
        private String message;

        @JsonProperty("data")
        private ResultInnerData data;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public ResultInnerData getData() {
            return data;
        }

        public void setData(ResultInnerData data) {
            this.data = data;
        }

        // Convenience method to get schema ID from nested data
        public Integer getSchemaId() {
            if (data != null && data.getId() != null) {
                return data.getId().intValue();
            }
            return null;
        }

        @Override
        public String toString() {
            return "ResultData{status='" + status + "', schemaId=" + getSchemaId() + '}';
        }
    }

    /**
     * Inner data object within result containing schema ID.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResultInnerData {

        @JsonProperty("id")
        private Double id;  // JSON returns as double (100264.0)

        @JsonProperty("version")
        private Integer version;

        public Double getId() {
            return id;
        }

        public void setId(Double id) {
            this.id = id;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }
    }
}

