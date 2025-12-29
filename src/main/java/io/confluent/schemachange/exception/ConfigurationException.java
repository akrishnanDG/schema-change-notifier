package io.confluent.schemachange.exception;

/**
 * Exception thrown when configuration validation fails.
 * This includes missing required properties, invalid values, and configuration conflicts.
 */
public class ConfigurationException extends RuntimeException {

    private final String propertyName;

    /**
     * Creates a new ConfigurationException.
     *
     * @param message The error message describing the configuration problem
     */
    public ConfigurationException(String message) {
        super(message);
        this.propertyName = null;
    }

    /**
     * Creates a new ConfigurationException for a specific property.
     *
     * @param message      The error message
     * @param propertyName The name of the invalid property
     */
    public ConfigurationException(String message, String propertyName) {
        super(message);
        this.propertyName = propertyName;
    }

    /**
     * Creates a new ConfigurationException with a cause.
     *
     * @param message The error message
     * @param cause   The underlying cause
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
        this.propertyName = null;
    }

    /**
     * @return The name of the invalid property, or null if not applicable
     */
    public String getPropertyName() {
        return propertyName;
    }
}

