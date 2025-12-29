package io.confluent.schemachange.registry;

import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.config.EnvironmentConfig;
import io.confluent.schemachange.exception.SchemaRegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static io.confluent.schemachange.config.KafkaConstants.*;

/**
 * Client for interacting with Confluent Schema Registry.
 * <p>
 * Supports multiple Schema Registries, one per Confluent Cloud environment.
 * Includes caching to reduce API calls for frequently accessed schemas.
 * </p>
 *
 * <h2>Thread Safety:</h2>
 * This class is thread-safe. The schema cache uses a ConcurrentHashMap.
 *
 * @see SchemaInfo
 */
public class SchemaRegistryService implements SchemaRegistryClient {

    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryService.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    private final Map<String, EnvironmentConfig> environmentConfigs;
    private final Map<String, SchemaInfo> schemaCache = new ConcurrentHashMap<>();

    /**
     * Creates a new SchemaRegistryService with the given configuration.
     *
     * @param config The application configuration containing environment settings
     */
    public SchemaRegistryService(@Nonnull AppConfig config) {
        this.environmentConfigs = config.getEnvironments();
        this.objectMapper = new ObjectMapper();
        
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(HTTP_CONNECT_TIMEOUT_SECONDS))
                .build();
        
        logger.info("Schema Registry service initialized for {} environment(s): {}", 
                environmentConfigs.size(), environmentConfigs.keySet());
    }

    /**
     * Creates a Basic Auth header value from API credentials.
     *
     * @param apiKey    The API key
     * @param apiSecret The API secret
     * @return The Authorization header value
     */
    private String createAuthHeader(@Nonnull String apiKey, @Nonnull String apiSecret) {
        String credentials = apiKey + ":" + apiSecret;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
    }

    /**
     * Checks if the service has configuration for a given environment.
     *
     * @param environmentId The environment ID
     * @return true if the environment is configured
     */
    public boolean hasEnvironment(@Nonnull String environmentId) {
        return environmentConfigs.containsKey(environmentId);
    }

    /**
     * Fetches schema by ID from Schema Registry for a specific environment.
     * Also fetches version info from /schemas/ids/{id}/versions endpoint.
     *
     * @param environmentId The environment ID
     * @param schemaId      The schema ID
     * @return SchemaInfo containing the schema details, or null if not found
     * @throws SchemaRegistryException if there's an error communicating with Schema Registry
     */
    @Nullable
    public SchemaInfo getSchemaById(@Nonnull String environmentId, int schemaId) {
        EnvironmentConfig envConfig = environmentConfigs.get(environmentId);
        if (envConfig == null) {
            logger.warn("No Schema Registry configured for environment: {}", environmentId);
            return null;
        }

        String cacheKey = environmentId + ":" + schemaId;
        SchemaInfo cached = schemaCache.get(cacheKey);
        if (cached != null) {
            logger.debug("Schema {}:{} found in cache", environmentId, schemaId);
            return cached;
        }

        try {
            String baseUrl = normalizeUrl(envConfig.getSchemaRegistryUrl());
            String authHeader = createAuthHeader(
                    envConfig.getSchemaRegistryApiKey(), 
                    envConfig.getSchemaRegistryApiSecret());

            String url = baseUrl + "/schemas/ids/" + schemaId;
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", authHeader)
                    .header("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)
                    .GET()
                    .timeout(Duration.ofSeconds(HTTP_READ_TIMEOUT_SECONDS))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode json = objectMapper.readTree(response.body());
                
                String schema = json.has("schema") ? json.get("schema").asText() : null;
                String schemaType = json.has("schemaType") ? json.get("schemaType").asText() : DEFAULT_SCHEMA_TYPE;
                Object references = json.has("references") ? json.get("references") : null;
                
                // Fetch version info
                VersionInfo versionInfo = fetchVersionInfo(envConfig, schemaId);
                
                SchemaInfo schemaInfo = new SchemaInfo(
                        environmentId,
                        schemaId,
                        versionInfo != null ? versionInfo.subject() : null,
                        versionInfo != null ? versionInfo.version() : null,
                        schema,
                        schemaType,
                        references
                );
                
                schemaCache.put(cacheKey, schemaInfo);
                
                logger.debug("Fetched schema {} from environment {} (subject={}, version={})", 
                        schemaId, environmentId, schemaInfo.subject(), schemaInfo.version());
                return schemaInfo;
                
            } else if (response.statusCode() == 404) {
                logger.warn("Schema {} not found in environment {}", schemaId, environmentId);
                return null;
            } else {
                throw new SchemaRegistryException(
                        String.format("Failed to fetch schema: %s", response.body()),
                        environmentId, schemaId, response.statusCode());
            }
        } catch (IOException e) {
            throw new SchemaRegistryException(
                    "Network error fetching schema: " + e.getMessage(),
                    environmentId, schemaId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryException(
                    "Request interrupted while fetching schema",
                    environmentId, schemaId, e);
        }
    }

    /**
     * Fetches version info for a schema ID.
     *
     * @param envConfig The environment configuration
     * @param schemaId  The schema ID
     * @return VersionInfo containing subject and version, or null if not found
     */
    @Nullable
    private VersionInfo fetchVersionInfo(@Nonnull EnvironmentConfig envConfig, int schemaId) {
        try {
            String baseUrl = normalizeUrl(envConfig.getSchemaRegistryUrl());
            String authHeader = createAuthHeader(
                    envConfig.getSchemaRegistryApiKey(), 
                    envConfig.getSchemaRegistryApiSecret());

            String url = baseUrl + "/schemas/ids/" + schemaId + "/versions";
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", authHeader)
                    .header("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)
                    .GET()
                    .timeout(Duration.ofSeconds(HTTP_READ_TIMEOUT_SECONDS))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode json = objectMapper.readTree(response.body());
                if (json.isArray() && !json.isEmpty()) {
                    JsonNode first = json.get(0);
                    String subject = first.has("subject") ? first.get("subject").asText() : null;
                    Integer version = first.has("version") ? first.get("version").asInt() : null;
                    return new VersionInfo(subject, version);
                }
            }
            return null;
        } catch (IOException | InterruptedException e) {
            logger.debug("Could not fetch version info for schema {}: {}", schemaId, e.getMessage());
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    /**
     * Fetches schema by subject and version from a specific environment.
     *
     * @param environmentId The environment ID
     * @param subject       The subject name
     * @param version       The schema version
     * @return SchemaInfo containing the schema details, or null if not found
     */
    @Nullable
    public SchemaInfo getSchemaBySubjectVersion(
            @Nonnull String environmentId, 
            @Nonnull String subject, 
            int version) {
        
        EnvironmentConfig envConfig = environmentConfigs.get(environmentId);
        if (envConfig == null) {
            logger.warn("No Schema Registry configured for environment: {}", environmentId);
            return null;
        }

        try {
            String baseUrl = normalizeUrl(envConfig.getSchemaRegistryUrl());
            String authHeader = createAuthHeader(
                    envConfig.getSchemaRegistryApiKey(), 
                    envConfig.getSchemaRegistryApiSecret());

            String url = baseUrl + "/subjects/" + subject + "/versions/" + version;
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", authHeader)
                    .header("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)
                    .GET()
                    .timeout(Duration.ofSeconds(HTTP_READ_TIMEOUT_SECONDS))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode json = objectMapper.readTree(response.body());
                
                Integer schemaId = json.has("id") ? json.get("id").asInt() : null;
                String schema = json.has("schema") ? json.get("schema").asText() : null;
                String schemaType = json.has("schemaType") ? json.get("schemaType").asText() : DEFAULT_SCHEMA_TYPE;
                Object references = json.has("references") ? json.get("references") : null;
                
                SchemaInfo schemaInfo = new SchemaInfo(
                        environmentId,
                        schemaId,
                        subject,
                        version,
                        schema,
                        schemaType,
                        references
                );
                
                if (schemaId != null) {
                    String cacheKey = environmentId + ":" + schemaId;
                    schemaCache.put(cacheKey, schemaInfo);
                }
                
                logger.debug("Fetched schema for subject {} version {} from environment {}", 
                        subject, version, environmentId);
                return schemaInfo;
                
            } else if (response.statusCode() == 404) {
                logger.warn("Schema for subject {} version {} not found in environment {}", 
                        subject, version, environmentId);
                return null;
            } else {
                throw new SchemaRegistryException(
                        String.format("Failed to fetch schema: %s", response.body()),
                        environmentId, null, response.statusCode());
            }
        } catch (IOException e) {
            throw new SchemaRegistryException(
                    "Network error fetching schema: " + e.getMessage(),
                    environmentId, null, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryException(
                    "Request interrupted while fetching schema",
                    environmentId, null, e);
        }
    }

    /**
     * Normalizes a URL by removing trailing slashes.
     *
     * @param url The URL to normalize
     * @return The normalized URL
     */
    private String normalizeUrl(@Nonnull String url) {
        return url.replaceAll("/$", "");
    }

    /**
     * Clears the schema cache.
     */
    public void clearCache() {
        schemaCache.clear();
        logger.info("Schema cache cleared");
    }

    /**
     * Gets the current cache size.
     *
     * @return The number of cached schemas
     */
    public int getCacheSize() {
        return schemaCache.size();
    }

    @Override
    public void close() {
        schemaCache.clear();
        logger.info("Schema Registry service closed");
    }

    /**
     * Internal record for version info from the versions endpoint.
     */
    private record VersionInfo(@Nullable String subject, @Nullable Integer version) {}

    /**
     * Immutable record containing schema information retrieved from Schema Registry.
     *
     * @param environmentId The environment ID where the schema was fetched from
     * @param schemaId      The unique schema ID
     * @param subject       The subject name
     * @param version       The schema version
     * @param schema        The schema content (JSON string)
     * @param schemaType    The schema type (AVRO, JSON, PROTOBUF)
     * @param references    Schema references for composite schemas
     */
    public record SchemaInfo(
            @Nullable String environmentId,
            @Nullable Integer schemaId,
            @Nullable String subject,
            @Nullable Integer version,
            @Nullable String schema,
            @Nonnull String schemaType,
            @Nullable Object references
    ) {
        /**
         * Creates a SchemaInfo with a default schema type of AVRO.
         */
        public SchemaInfo {
            if (schemaType == null) {
                schemaType = DEFAULT_SCHEMA_TYPE;
            }
        }

        /**
         * Gets the schema content.
         * @return The schema as a string
         * @deprecated Use {@link #schema()} instead
         */
        @Deprecated
        public String getSchema() {
            return schema;
        }

        /**
         * Gets the schema type.
         * @return The schema type
         * @deprecated Use {@link #schemaType()} instead
         */
        @Deprecated
        public String getSchemaType() {
            return schemaType;
        }

        /**
         * Gets the schema version.
         * @return The version number
         * @deprecated Use {@link #version()} instead
         */
        @Deprecated
        public Integer getVersion() {
            return version;
        }

        /**
         * Gets the schema references.
         * @return The references object
         * @deprecated Use {@link #references()} instead
         */
        @Deprecated
        public Object getReferences() {
            return references;
        }
    }
}
