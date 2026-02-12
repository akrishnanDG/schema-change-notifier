package io.confluent.schemachange.health;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight HTTP server for health checks and metrics.
 * Uses JDK built-in {@link HttpServer} with zero additional dependencies.
 *
 * <ul>
 *   <li>GET /health - Returns {"status":"UP"} (200) or {"status":"DOWN"} (503)</li>
 *   <li>GET /metrics - Returns JSON with application metrics</li>
 * </ul>
 */
public class HealthServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;
    private final AtomicBoolean healthy;
    private final AtomicLong eventsConsumed;
    private final AtomicLong eventsProcessed;
    private final AtomicLong notificationsProduced;
    private final AtomicLong duplicatesSkipped;
    private final long startTime;

    public HealthServer(
            int port,
            AtomicBoolean healthy,
            AtomicLong eventsConsumed,
            AtomicLong eventsProcessed,
            AtomicLong notificationsProduced,
            AtomicLong duplicatesSkipped) throws IOException {

        this.healthy = healthy;
        this.eventsConsumed = eventsConsumed;
        this.eventsProcessed = eventsProcessed;
        this.notificationsProduced = notificationsProduced;
        this.duplicatesSkipped = duplicatesSkipped;
        this.startTime = System.currentTimeMillis();

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> {
            boolean isUp = healthy.get();
            int statusCode = isUp ? 200 : 503;
            String status = isUp ? "UP" : "DOWN";
            String body = "{\"status\":\"" + status + "\"}";

            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        server.createContext("/metrics", exchange -> {
            long uptimeSeconds = (System.currentTimeMillis() - startTime) / 1000;
            String body = String.format(
                    "{\"eventsConsumed\":%d,\"eventsProcessed\":%d,\"notificationsProduced\":%d,\"duplicatesSkipped\":%d,\"uptimeSeconds\":%d}",
                    eventsConsumed.get(),
                    eventsProcessed.get(),
                    notificationsProduced.get(),
                    duplicatesSkipped.get(),
                    uptimeSeconds);

            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
    }

    public void start() {
        server.start();
        logger.info("Health server started on port {}", server.getAddress().getPort());
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
        logger.info("Health server stopped");
    }
}
