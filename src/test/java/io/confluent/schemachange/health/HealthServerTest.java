package io.confluent.schemachange.health;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HealthServer Tests")
class HealthServerTest {

    private HealthServer healthServer;
    private AtomicBoolean healthy;
    private AtomicLong eventsConsumed;
    private AtomicLong eventsProcessed;
    private AtomicLong notificationsProduced;
    private AtomicLong duplicatesSkipped;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        healthy = new AtomicBoolean(true);
        eventsConsumed = new AtomicLong(0);
        eventsProcessed = new AtomicLong(0);
        notificationsProduced = new AtomicLong(0);
        duplicatesSkipped = new AtomicLong(0);

        // Use port 0 for random available port
        healthServer = new HealthServer(0, healthy, eventsConsumed, eventsProcessed,
                notificationsProduced, duplicatesSkipped);
        healthServer.start();
        port = healthServer.getPort();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (healthServer != null) {
            healthServer.close();
        }
    }

    @Test
    @DisplayName("Health endpoint should return UP when healthy")
    void healthEndpointShouldReturnUp() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/health")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"UP\""));
    }

    @Test
    @DisplayName("Health endpoint should return DOWN when unhealthy")
    void healthEndpointShouldReturnDown() throws Exception {
        healthy.set(false);

        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/health")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(503, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"DOWN\""));
    }

    @Test
    @DisplayName("Metrics endpoint should return JSON with counters")
    void metricsEndpointShouldReturnCounters() throws Exception {
        eventsConsumed.set(100);
        eventsProcessed.set(50);
        notificationsProduced.set(45);
        duplicatesSkipped.set(5);

        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/metrics")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String body = response.body();
        assertTrue(body.contains("\"eventsConsumed\":100"));
        assertTrue(body.contains("\"eventsProcessed\":50"));
        assertTrue(body.contains("\"notificationsProduced\":45"));
        assertTrue(body.contains("\"duplicatesSkipped\":5"));
        assertTrue(body.contains("\"uptimeSeconds\":"));
    }

    @Test
    @DisplayName("Metrics endpoint should return zero counters initially")
    void metricsEndpointShouldReturnZeroInitially() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/metrics")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"eventsConsumed\":0"));
    }

    @Test
    @DisplayName("Health endpoint should return correct content type")
    void healthEndpointShouldReturnJsonContentType() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/health")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertTrue(response.headers().firstValue("Content-Type").orElse("").contains("application/json"));
    }
}
