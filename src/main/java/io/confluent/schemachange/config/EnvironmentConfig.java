package io.confluent.schemachange.config;

import io.confluent.schemachange.exception.ConfigurationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Configuration for a single Confluent Cloud environment's Schema Registry.
 * <p>
 * Each monitored environment requires its own Schema Registry credentials
 * since Schema Registries are environment-scoped in Confluent Cloud.
 * </p>
 *
 * @see AppConfig
 */
public class EnvironmentConfig {

    private String environmentId;
    private String schemaRegistryUrl;
    private String schemaRegistryApiKey;
    private String schemaRegistryApiSecret;

    /**
     * Default constructor for deserialization.
     */
    public EnvironmentConfig() {
    }

    /**
     * Creates a new EnvironmentConfig with all required fields.
     *
     * @param environmentId         The environment ID (e.g., "env-abc123")
     * @param schemaRegistryUrl     The Schema Registry URL
     * @param schemaRegistryApiKey  The Schema Registry API key
     * @param schemaRegistryApiSecret The Schema Registry API secret
     */
    public EnvironmentConfig(
            @Nonnull String environmentId,
            @Nonnull String schemaRegistryUrl,
            @Nonnull String schemaRegistryApiKey,
            @Nonnull String schemaRegistryApiSecret) {
        this.environmentId = environmentId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.schemaRegistryApiKey = schemaRegistryApiKey;
        this.schemaRegistryApiSecret = schemaRegistryApiSecret;
    }

    /**
     * Gets the environment ID.
     *
     * @return The environment ID (e.g., "env-abc123")
     */
    @Nullable
    public String getEnvironmentId() {
        return environmentId;
    }

    /**
     * Sets the environment ID.
     *
     * @param environmentId The environment ID
     */
    public void setEnvironmentId(@Nonnull String environmentId) {
        this.environmentId = environmentId;
    }

    /**
     * Gets the Schema Registry URL.
     *
     * @return The Schema Registry URL (e.g., "https://psrc-xxx.region.aws.confluent.cloud")
     */
    @Nullable
    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    /**
     * Sets the Schema Registry URL.
     *
     * @param schemaRegistryUrl The Schema Registry URL
     */
    public void setSchemaRegistryUrl(@Nonnull String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    /**
     * Gets the Schema Registry API key.
     *
     * @return The API key
     */
    @Nullable
    public String getSchemaRegistryApiKey() {
        return schemaRegistryApiKey;
    }

    /**
     * Sets the Schema Registry API key.
     *
     * @param schemaRegistryApiKey The API key
     */
    public void setSchemaRegistryApiKey(@Nonnull String schemaRegistryApiKey) {
        this.schemaRegistryApiKey = schemaRegistryApiKey;
    }

    /**
     * Gets the Schema Registry API secret.
     *
     * @return The API secret
     */
    @Nullable
    public String getSchemaRegistryApiSecret() {
        return schemaRegistryApiSecret;
    }

    /**
     * Sets the Schema Registry API secret.
     *
     * @param schemaRegistryApiSecret The API secret
     */
    public void setSchemaRegistryApiSecret(@Nonnull String schemaRegistryApiSecret) {
        this.schemaRegistryApiSecret = schemaRegistryApiSecret;
    }

    /**
     * Validates that all required configuration is present.
     *
     * @throws ConfigurationException if required configuration is missing
     */
    public void validate() {
        StringBuilder errors = new StringBuilder();

        if (isNullOrEmpty(environmentId)) {
            errors.append("Environment ID is required.\n");
        }
        if (isNullOrEmpty(schemaRegistryUrl)) {
            errors.append("Schema Registry URL is required for environment: ").append(environmentId).append("\n");
        }
        if (isNullOrEmpty(schemaRegistryApiKey)) {
            errors.append("Schema Registry API key is required for environment: ").append(environmentId).append("\n");
        }
        if (isNullOrEmpty(schemaRegistryApiSecret)) {
            errors.append("Schema Registry API secret is required for environment: ").append(environmentId).append("\n");
        }

        if (!errors.isEmpty()) {
            throw new ConfigurationException(errors.toString().trim());
        }
    }

    private boolean isNullOrEmpty(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }

    @Override
    public String toString() {
        return "EnvironmentConfig{" +
                "environmentId='" + environmentId + '\'' +
                ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
                '}';
    }
}
