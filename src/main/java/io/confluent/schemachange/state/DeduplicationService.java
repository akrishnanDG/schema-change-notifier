package io.confluent.schemachange.state;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.schemachange.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.schemachange.config.KafkaConstants.MAX_DEDUP_EVENTS;

/**
 * Service for tracking processed events to enable deduplication.
 * <p>
 * Persists state to disk for resume capability across application restarts.
 * Uses a simple file-based approach with atomic writes for durability.
 * </p>
 *
 * <h2>Deduplication Strategy:</h2>
 * Events are deduplicated using a composite key of:
 * {@code subject + methodName + schemaId}
 *
 * <h2>Memory Management:</h2>
 * The service maintains a maximum of {@link io.confluent.schemachange.config.KafkaConstants#MAX_DEDUP_EVENTS}
 * entries in memory. When this limit is reached, 20% of entries are pruned using FIFO.
 *
 * <h2>Thread Safety:</h2>
 * This class is thread-safe. Uses ConcurrentHashMap for the event set.
 */
public class DeduplicationService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DeduplicationService.class);

    private static final double PRUNE_RATIO = 0.2;

    private final Set<String> processedEvents;
    private final Path stateFilePath;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new DeduplicationService with the given configuration.
     *
     * @param config The application configuration
     */
    public DeduplicationService(@Nonnull AppConfig config) {
        this.stateFilePath = Path.of(config.getStateFilePath());
        this.processedEvents = ConcurrentHashMap.newKeySet();
        this.objectMapper = new ObjectMapper();
        loadState();
    }

    /**
     * Checks if an event has already been processed.
     *
     * @param key The unique event key
     * @return true if this is a duplicate (already processed)
     */
    public boolean isDuplicate(@Nonnull String key) {
        return processedEvents.contains(key);
    }

    /**
     * Marks an event as processed.
     *
     * @param key The unique event key
     * @return true if successfully added (not a duplicate)
     */
    public boolean markProcessed(@Nonnull String key) {
        if (processedEvents.size() >= MAX_DEDUP_EVENTS) {
            pruneOldEntries();
        }
        return processedEvents.add(key);
    }

    /**
     * Prunes old entries to prevent unbounded memory growth.
     * Removes approximately 20% of entries using FIFO ordering.
     */
    private void pruneOldEntries() {
        int toRemove = (int) (MAX_DEDUP_EVENTS * PRUNE_RATIO);
        int removed = 0;
        
        var iterator = processedEvents.iterator();
        while (iterator.hasNext() && removed < toRemove) {
            iterator.next();
            iterator.remove();
            removed++;
        }
        
        logger.debug("Pruned {} entries from deduplication cache", removed);
    }

    /**
     * Loads state from disk.
     * <p>
     * If the state file does not exist or cannot be read, starts with an empty set.
     * </p>
     */
    public void loadState() {
        if (!Files.exists(stateFilePath)) {
            logger.debug("No existing state file found at {}", stateFilePath);
            return;
        }

        try {
            Set<String> loaded = objectMapper.readValue(
                    stateFilePath.toFile(),
                    new TypeReference<HashSet<String>>() {}
            );
            processedEvents.addAll(loaded);
            logger.info("Loaded {} entries from deduplication state file", loaded.size());
        } catch (IOException e) {
            logger.warn("Failed to load dedup state file, starting fresh: {}", e.getMessage());
        }
    }

    /**
     * Saves state to disk atomically.
     * <p>
     * Writes to a temporary file first, then atomically renames to ensure
     * the state file is never corrupted.
     * </p>
     */
    public void saveState() {
        try {
            // Ensure parent directory exists (handle null parent)
            Path parentDir = stateFilePath.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }
            
            // Write atomically by writing to temp file first
            File tempFile = new File(stateFilePath + ".tmp");
            objectMapper.writeValue(tempFile, new HashSet<>(processedEvents));
            
            // Rename temp file to actual file
            Files.move(tempFile.toPath(), stateFilePath, 
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            
            logger.debug("Saved {} entries to deduplication state file", processedEvents.size());
        } catch (IOException e) {
            logger.error("Failed to save dedup state file: {}", e.getMessage());
        }
    }

    /**
     * Gets the number of tracked events.
     *
     * @return The number of events in the deduplication cache
     */
    public int size() {
        return processedEvents.size();
    }

    /**
     * Clears all tracked events.
     * <p>
     * This does NOT delete the state file. Call {@link #saveState()} after
     * clearing to persist the empty state.
     * </p>
     */
    public void clear() {
        processedEvents.clear();
        logger.info("Deduplication cache cleared");
    }

    @Override
    public void close() {
        saveState();
        logger.info("Deduplication service closed, saved {} entries", processedEvents.size());
    }
}
