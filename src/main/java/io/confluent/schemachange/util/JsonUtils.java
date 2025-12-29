package io.confluent.schemachange.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for JSON serialization and deserialization.
 */
public final class JsonUtils {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private JsonUtils() {
        // Utility class, no instantiation
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        
        // Register Java 8 time module
        mapper.registerModule(new JavaTimeModule());
        
        // Configure deserialization
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        
        // Configure serialization
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        
        return mapper;
    }

    /**
     * Deserialize JSON string to object.
     *
     * @param json  The JSON string
     * @param clazz The target class
     * @param <T>   The target type
     * @return The deserialized object, or null if deserialization fails
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize JSON to {}: {}", clazz.getSimpleName(), e.getMessage());
            logger.debug("JSON content: {}", json);
            return null;
        }
    }

    /**
     * Serialize object to JSON string.
     *
     * @param object The object to serialize
     * @return The JSON string, or null if serialization fails
     */
    public static String toJson(Object object) {
        if (object == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to JSON: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Serialize object to pretty-printed JSON string.
     *
     * @param object The object to serialize
     * @return The pretty-printed JSON string, or null if serialization fails
     */
    public static String toPrettyJson(Object object) {
        if (object == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to pretty JSON: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Get the shared ObjectMapper instance.
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}

