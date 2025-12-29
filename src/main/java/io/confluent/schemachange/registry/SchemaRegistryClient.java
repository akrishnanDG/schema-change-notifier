package io.confluent.schemachange.registry;

import io.confluent.schemachange.registry.SchemaRegistryService.SchemaInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for Schema Registry operations.
 * Enables easier testing with mock implementations.
 */
public interface SchemaRegistryClient extends AutoCloseable {

    /**
     * Checks if the service has configuration for a given environment.
     *
     * @param environmentId The environment ID
     * @return true if the environment is configured
     */
    boolean hasEnvironment(@Nonnull String environmentId);

    /**
     * Fetches schema by ID from Schema Registry for a specific environment.
     *
     * @param environmentId The environment ID
     * @param schemaId      The schema ID
     * @return SchemaInfo containing the schema details, or null if not found
     */
    @Nullable
    SchemaInfo getSchemaById(@Nonnull String environmentId, int schemaId);

    /**
     * Fetches schema by subject and version from a specific environment.
     *
     * @param environmentId The environment ID
     * @param subject       The subject name
     * @param version       The schema version
     * @return SchemaInfo containing the schema details, or null if not found
     */
    @Nullable
    SchemaInfo getSchemaBySubjectVersion(@Nonnull String environmentId, @Nonnull String subject, int version);

    /**
     * Clears the schema cache.
     */
    void clearCache();

    /**
     * Gets the current cache size.
     *
     * @return The number of cached schemas
     */
    int getCacheSize();
}

