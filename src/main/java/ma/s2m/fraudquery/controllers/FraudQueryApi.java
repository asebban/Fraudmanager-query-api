package ma.s2m.fraudquery.controllers;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.impl.Headers;
import ma.s2m.auth.query.FraudQueryRequest;
import ma.s2m.auth.query.FraudQueryResponse;
import ma.s2m.auth.query.Indicator;
import ma.s2m.serializer.SerializationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Minimal reactive (WebFlux) controller that sends a request to NATS (request/reply)
 * and returns the deserialized response.
 *
 * Endpoint: GET /api/fraud/query?key=xxx&timeframe=yyy
 */
@RestController
@RequestMapping(path = "/api/fraud", produces = MediaType.APPLICATION_JSON_VALUE)
public class FraudQueryApi {

    private final Connection nats;
    @SuppressWarnings("unused")
    private final Duration timeout;
    private String topic;
    private Logger logger = LoggerFactory.getLogger(FraudQueryApi.class);

    private static final Long SECONDS_IN_MILLIS = 1000L;
    private static final Long MINUTES_IN_MILLIS = 60 * SECONDS_IN_MILLIS;
    private static final Long HOURS_IN_MILLIS = 60 * MINUTES_IN_MILLIS;
    private static final Long DAYS_IN_MILLIS = 24 * HOURS_IN_MILLIS;
    private static final Long WEEKS_IN_MILLIS = 7 * DAYS_IN_MILLIS;
    private static final Long MONTHS_IN_MILLIS = 30 * DAYS_IN_MILLIS; // approximated
    private static final Long YEARS_IN_MILLIS = 365 * DAYS_IN_MILLIS; // approximated 

    public FraudQueryApi(Connection nats,
                         @Value("${fraud.query.topic:fraud.query}") String topic,
                         Duration timeout) {
        this.nats = nats;
        this.timeout = timeout;
        this.topic = topic;
    }

    @GetMapping("/query")
    public Mono<ResponseEntity<FraudQueryResponse>> query(
            @RequestParam("key") String key,
            @RequestParam("timeframe") String timeframe,
            @RequestParam("subject") String subject) {

        Message reply;
        UUID uuid = UUID.randomUUID();

        // Validate inputs early
        if (!StringUtils.hasText(key) || !StringUtils.hasText(subject)) {
            return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST).build());
        }

        try {
            // 1) Build request DTO
            FraudQueryRequest req = new FraudQueryRequest();
            req.setKey(key);
            req.setTimeframe(parseDurationToMillis(timeframe));
            req.setSubject(subject);

            // 2) Serialize to byte[]
            byte[] payload = SerializationManager.serialize(req);

            Headers headers = new Headers();
            headers.put("x-correlation-id", uuid.toString());

            long clientPublishMs = System.currentTimeMillis();
            headers.put("x-client-publish-ts-ms", Long.toString(clientPublishMs));

            long natsPublishTime = System.currentTimeMillis();
            reply = nats.request(topic, headers, payload, Duration.ofMillis(1000));
            long endNatsPublishTime = System.currentTimeMillis();
            logger.info("NATS reply time (ms): " + (endNatsPublishTime - natsPublishTime));

            if (reply == null) {
                return Mono.just(mapError(new RuntimeException("The key '" + key + "' was not found.")));
            }
            
            try {
                // 3) Deserialize response into FraudQueryResponse
                FraudQueryResponse response = (FraudQueryResponse) SerializationManager.deserialize(reply.getData());
                logger.info("[{}] Response receved", response.getCorrelationId());
                return Mono.just(ResponseEntity.ok(response));
            } catch (Exception e) {
                logger.error("Error occurred while processing NATS request", e);
                return Mono.just(mapError(e));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error occurred while processing NATS request", e);
            return Mono.just(mapError(e));
        }
    }

    private ResponseEntity<FraudQueryResponse> mapError(Throwable t) {
        String message = Objects.requireNonNullElse(t.getMessage(), t.toString());
        FraudQueryResponse error = new FraudQueryResponse();
        error.setKey("ERROR");
        error.setTimeframe(0L);
        Map<String, Indicator> m = new HashMap<>();
        Indicator rec = new Indicator();
        rec.setCount(0L);
        rec.setAmount(0.0);
        m.put("error: " + message, rec);
        error.setRecords(m);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(error);
    }

    public static Long parseDurationToMillis(String input) {
        if (input == null || input.isBlank()) {
            return 0L;
        }

        // Normalisation et séparation
        String[] parts = input.trim().split("\\s+");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid format, expected: '<number> <timeunit>'");
        }

        long value;
        try {
            value = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number: " + parts[0]);
        }

        String unit = parts[1].toLowerCase();
        switch (unit) {
            case "second":
            case "seconds":
                return value * SECONDS_IN_MILLIS;
            case "minute":
            case "minutes":
                return value * MINUTES_IN_MILLIS;
            case "hour":
            case "hours":
                return value * HOURS_IN_MILLIS;
            case "day":
            case "days":
                return value * DAYS_IN_MILLIS;
            case "week":
            case "weeks":
                return value * WEEKS_IN_MILLIS;
            case "month":
            case "months":
                return value * MONTHS_IN_MILLIS;
            case "year":
            case "years":
                return value * YEARS_IN_MILLIS;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }
}

// ===== NATS Configuration =====
@Configuration
class NatsConfig {

    @Bean(destroyMethod = "close")
    public Connection natsConnection(@Value("${nats.url:nats://localhost:4222}") String url) throws Exception {
        // Create a singleton NATS connection for the app; Spring will close it on shutdown
        return Nats.connect(url);
    }

    @Bean
    public Duration fraudQueryTimeout(@Value("${fraud.query.timeout:5s}") String timeoutString) {
        return Duration.parse("PT" + timeoutString.toUpperCase());
    }
}

// ===== Optional: health ping component to verify NATS at startup (non-blocking) =====
@Component
class NatsHealthLogger {
    public NatsHealthLogger(Connection nats) {
        // This is optional and safe; remove if unwanted
        if (nats.getStatus() != Connection.Status.CONNECTED) {
            System.err.println("[NATS] Not connected at startup: " + nats.getStatus());
        }
    }
}
