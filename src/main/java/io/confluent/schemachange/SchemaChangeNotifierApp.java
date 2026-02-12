package io.confluent.schemachange;

import io.confluent.schemachange.cli.SchemaChangeNotifierCommand;
import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.consumer.AuditLogConsumer;
import io.confluent.schemachange.health.HealthServer;
import io.confluent.schemachange.model.AuditLogEvent;
import io.confluent.schemachange.model.SchemaChangeNotification;
import io.confluent.schemachange.processor.SchemaChangeProcessor;
import io.confluent.schemachange.producer.NotificationProducer;
import io.confluent.schemachange.registry.SchemaRegistryService;
import io.confluent.schemachange.state.DeduplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main application class for the Schema Change Notifier.
 * 
 * This tool monitors Confluent Cloud audit logs for schema registry operations
 * and produces change notification events to a target Kafka topic.
 */
public class SchemaChangeNotifierApp {

    private static final Logger logger = LoggerFactory.getLogger(SchemaChangeNotifierApp.class);

    private final AppConfig config;
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    // Metrics
    private final AtomicLong eventsConsumed = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong notificationsProduced = new AtomicLong(0);
    private final AtomicLong duplicatesSkipped = new AtomicLong(0);

    public SchemaChangeNotifierApp(AppConfig config) {
        this.config = config;
    }

    /**
     * Run the application.
     */
    public void run() {
        logger.info("Starting Schema Change Notifier in {} mode...", config.getProcessingMode());
        if (config.isDryRun()) {
            logger.info("Dry run mode - notifications will be logged but not produced");
        }

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));

        HealthServer healthServer = null;
        ExecutorService executor = null;

        try (
                AuditLogConsumer consumer = new AuditLogConsumer(config);
                SchemaRegistryService schemaRegistry = new SchemaRegistryService(config);
                NotificationProducer producer = new NotificationProducer(config);
                DeduplicationService deduplication = new DeduplicationService(config)
        ) {
            SchemaChangeProcessor processor = new SchemaChangeProcessor(config, schemaRegistry);

            // Start health server if configured
            if (config.getHealthPort() > 0) {
                try {
                    healthServer = new HealthServer(config.getHealthPort(), running,
                            eventsConsumed, eventsProcessed, notificationsProduced, duplicatesSkipped);
                    healthServer.start();
                } catch (Exception e) {
                    logger.warn("Failed to start health server on port {}: {}", config.getHealthPort(), e.getMessage());
                }
            }

            // Create thread pool for parallel processing if configured
            int threads = config.getProcessingThreads();
            if (threads > 1) {
                executor = Executors.newFixedThreadPool(threads);
                logger.info("Parallel processing enabled with {} threads", threads);
            }

            long lastStatusLog = System.currentTimeMillis();

            while (running.get() && consumer.isRunning()) {
                try {
                    List<AuditLogEvent> events = consumer.poll();

                    if (events.isEmpty()) {
                        continue;
                    }

                    eventsConsumed.addAndGet(events.size());

                    if (executor != null && events.size() > 1) {
                        // Parallel processing
                        List<CompletableFuture<Void>> futures = new ArrayList<>();
                        for (AuditLogEvent event : events) {
                            futures.add(CompletableFuture.runAsync(
                                    () -> processEvent(event, processor, producer, deduplication), executor));
                        }
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                .get(60, TimeUnit.SECONDS);
                    } else {
                        // Sequential processing
                        for (AuditLogEvent event : events) {
                            processEvent(event, processor, producer, deduplication);
                        }
                    }

                    consumer.commitSync();

                    long now = System.currentTimeMillis();
                    if (now - lastStatusLog > 60_000) {
                        logStatus();
                        lastStatusLog = now;
                    }

                } catch (Exception e) {
                    logger.error("Error in main loop: {}", e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            logger.error("Fatal error: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if (healthServer != null) {
                healthServer.close();
            }
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            logFinalStatus();
        }
    }

    /**
     * Process a single audit log event.
     */
    private void processEvent(
            AuditLogEvent event,
            SchemaChangeProcessor processor,
            NotificationProducer producer,
            DeduplicationService deduplication
    ) {
        // Check for duplicates
        String dedupKey = processor.createDeduplicationKey(event);
        if (deduplication.isDuplicate(dedupKey)) {
            duplicatesSkipped.incrementAndGet();
            logger.debug("Skipping duplicate event: {}", event.getId());
            return;
        }

        // Process the event
        Optional<SchemaChangeNotification> notification = processor.process(event);
        
        if (notification.isPresent()) {
            eventsProcessed.incrementAndGet();
            
            // Produce notification
            boolean sent = producer.send(notification.get());
            
            if (sent) {
                notificationsProduced.incrementAndGet();
                // Mark as processed for deduplication
                deduplication.markProcessed(dedupKey);
            } else {
                logger.warn("Failed to produce notification for event: {}", event.getId());
            }
        }
    }

    /**
     * Log current status.
     */
    private void logStatus() {
        logger.info("Status - Consumed: {}, Processed: {}, Produced: {}, Duplicates: {}",
                eventsConsumed.get(),
                eventsProcessed.get(),
                notificationsProduced.get(),
                duplicatesSkipped.get());
    }

    /**
     * Log final status on shutdown.
     */
    private void logFinalStatus() {
        logger.info("=== Final Status ===");
        logger.info("Total events consumed: {}", eventsConsumed.get());
        logger.info("Total events processed: {}", eventsProcessed.get());
        logger.info("Total notifications produced: {}", notificationsProduced.get());
        logger.info("Total duplicates skipped: {}", duplicatesSkipped.get());
        logger.info("Schema Change Notifier stopped");
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        SchemaChangeNotifierCommand command = new SchemaChangeNotifierCommand();
        
        CommandLine commandLine = new CommandLine(command);
        
        // Parse arguments first to check for help/version
        CommandLine.ParseResult parseResult;
        try {
            parseResult = commandLine.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            commandLine.usage(System.err);
            System.exit(1);
            return;
        }

        // Handle help and version
        if (commandLine.isUsageHelpRequested()) {
            commandLine.usage(System.out);
            return;
        }
        if (commandLine.isVersionHelpRequested()) {
            commandLine.printVersionHelp(System.out);
            return;
        }

        try {
            // Build configuration
            AppConfig config = command.buildConfig();
            
            // Validate configuration
            config.validate();
            
            // Create and run application
            SchemaChangeNotifierApp app = new SchemaChangeNotifierApp(config);
            app.run();
            
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

