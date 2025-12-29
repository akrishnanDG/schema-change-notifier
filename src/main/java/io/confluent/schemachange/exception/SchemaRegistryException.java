package io.confluent.schemachange.exception;

/**
 * Exception thrown when Schema Registry operations fail.
 * This includes connection failures, authentication errors, and schema not found scenarios.
 */
public class SchemaRegistryException extends RuntimeException {

    private final String environmentId;
    private final Integer schemaId;
    private final int statusCode;

    /**
     * Creates a new SchemaRegistryException.
     *
     * @param message The error message
     */
    public SchemaRegistryException(String message) {
        super(message);
        this.environmentId = null;
        this.schemaId = null;
        this.statusCode = -1;
    }

    /**
     * Creates a new SchemaRegistryException with a cause.
     *
     * @param message The error message
     * @param cause   The underlying cause
     */
    public SchemaRegistryException(String message, Throwable cause) {
        super(message, cause);
        this.environmentId = null;
        this.schemaId = null;
        this.statusCode = -1;
    }

    /**
     * Creates a new SchemaRegistryException with full context.
     *
     * @param message       The error message
     * @param environmentId The environment ID where the error occurred
     * @param schemaId      The schema ID being accessed
     * @param statusCode    The HTTP status code from Schema Registry
     */
    public SchemaRegistryException(String message, String environmentId, Integer schemaId, int statusCode) {
        super(message);
        this.environmentId = environmentId;
        this.schemaId = schemaId;
        this.statusCode = statusCode;
    }

    /**
     * Creates a new SchemaRegistryException with full context and cause.
     *
     * @param message       The error message
     * @param environmentId The environment ID where the error occurred
     * @param schemaId      The schema ID being accessed
     * @param cause         The underlying cause
     */
    public SchemaRegistryException(String message, String environmentId, Integer schemaId, Throwable cause) {
        super(message, cause);
        this.environmentId = environmentId;
        this.schemaId = schemaId;
        this.statusCode = -1;
    }

    /**
     * @return The environment ID where the error occurred, or null if not applicable
     */
    public String getEnvironmentId() {
        return environmentId;
    }

    /**
     * @return The schema ID being accessed, or null if not applicable
     */
    public Integer getSchemaId() {
        return schemaId;
    }

    /**
     * @return The HTTP status code, or -1 if not applicable
     */
    public int getStatusCode() {
        return statusCode;
    }
}

