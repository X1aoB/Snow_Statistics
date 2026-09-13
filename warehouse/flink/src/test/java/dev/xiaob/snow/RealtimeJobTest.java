package dev.xiaob.snow;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RealtimeJobTest {
    String fixture = "{\"source\":\"synthetic\",\"seq\":1,\"accepted_at\":\"2026-01-01T16:00:01Z\",\"event\":{\"schema_version\":1,\"event_id\":\"00000000-0000-4000-8000-000000000001\",\"app\":\"mywebsite\",\"event_type\":\"page_view\",\"occurred_at\":\"2026-01-01T16:00:00Z\",\"path\":\"/\"}}";
    @Test void timezoneAndAllowlist() throws Exception {
        var out = RealtimeJob.normalize(fixture, "synthetic");
        assertEquals("2026-01-02", out.path("business_date").asText());
        assertEquals("event:00000000-0000-4000-8000-000000000001", out.path("business_key").asText());
        assertFalse(RealtimeJob.dorisRow(out.toString()).contains("_event"));
        assertEquals(Long.MAX_VALUE - 1, out.path("business_version").asLong());
    }
    @Test void provenanceCannotCross() {
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(fixture, "real"));
    }
    @Test void realFingerprintDoesNotRetainFullEventAndWindowDoesNotRefresh() throws Exception {
        String real = fixture.replace("synthetic", "real");
        String hash = RealtimeJob.normalize(real, "real").path("_event_json").asText();
        assertTrue(hash.matches("[0-9a-f]{64}"));
        assertFalse(hash.contains("path"));
        var origin = java.time.Instant.parse("2026-01-01T16:00:01Z");
        var expiry = origin.plus(java.time.Duration.ofDays(7));
        assertDoesNotThrow(() -> RealtimeJob.realWindow(origin, origin, expiry, origin.plusSeconds(1)));
        assertThrows(IllegalStateException.class, () -> RealtimeJob.realWindow(origin, origin, expiry, expiry));
        assertThrows(IllegalStateException.class, () -> RealtimeJob.realWindow(origin, origin, expiry.plusSeconds(1), origin));
        assertThrows(IllegalStateException.class, () -> RealtimeJob.realWindow(origin.minusSeconds(1), origin, expiry, origin));
    }
    @Test void oversizedVersionCannotWrapToV1() {
        assertThrows(IllegalArgumentException.class,
            () -> RealtimeJob.normalize(fixture.replace("\"schema_version\":1", "\"schema_version\":4294967297"), "synthetic"));
    }
    @Test void realJobCannotReplaceStorageEpochWindowOrLane() {
        var origin = java.time.Instant.parse("2026-09-13T12:00:00Z");
        var until = origin.plus(java.time.Duration.ofDays(7));
        String generation = "00000000-0000-4000-8000-000000000001";
        assertDoesNotThrow(() -> RealtimeJob.realEpoch("real-start-01", generation, origin, until, origin, until, "real_start_01"));
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.realEpoch("real-start-01", generation, origin, until, origin.minusSeconds(1), until, "real_start_01"));
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.realEpoch("real-start-01", generation, origin, until, origin, until.plusSeconds(1), "real_start_01"));
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.realEpoch("real-start-01", generation, origin, until, origin, until, "another_lane"));
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.realEpoch("real-start-01", "0-0-0-0-0", origin, until, origin, until, "real_start_01"));
    }
    @Test void realDiagnosticAndKafkaReplayPreserveOriginalExpiryBasis() throws Exception {
        var normalized = RealtimeJob.normalize(fixture.replace("synthetic", "real"), "real");
        var value = RealtimeJob.diagnostic(normalized.toString(), "event_duplicate");
        var row = RealtimeJob.JSON.readTree(value);
        assertEquals("real", row.path("source").asText());
        assertEquals("2026-01-01T16:00:01Z", row.path("accepted_at").asText());
        assertEquals(java.time.Instant.parse("2026-01-01T16:00:01Z").toEpochMilli(),
            RealtimeJob.originalKafkaTimestamp(value, System.currentTimeMillis()));
        assertFalse(value.contains("path"));
        assertFalse(value.contains("anonymous_id"));
        assertThrows(IllegalArgumentException.class,
            () -> RealtimeJob.originalKafkaTimestamp("{\"source\":\"real\"}", 0L));
    }
    @Test void acceptedMicrosecondsAndAnonymousUuidAreValid() throws Exception {
        var envelope = (com.fasterxml.jackson.databind.node.ObjectNode) RealtimeJob.JSON.readTree(fixture);
        envelope.put("accepted_at", "2026-09-11T11:07:55.220556+00:00");
        ((com.fasterxml.jackson.databind.node.ObjectNode) envelope.path("event")).put("anonymous_id", "33ae45bb-b879-5c43-bd8d-e9cd38a67110");
        assertEquals("mywebsite", RealtimeJob.normalize(envelope.toString(), "synthetic").path("app").asText());
    }
    @Test void malformedAndPrivateFieldsAreQuarantined() {
        for (String value : new String[]{fixture.replace("\"path\":\"/\"", "\"path\":\"/?key=private\""),
                fixture.replace("\"path\":\"/\"", "\"path\":\"/\",\"body\":\"private\""),
                fixture.replace("\"seq\":1", "\"seq\":0"), fixture.replace("\"seq\":1", "\"seq\":1.5"),
                fixture.replace("00000000-0000-4000-8000-000000000001", "invalid")})
            assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(value, "synthetic"));
    }
    @Test void completionCannotInventSuccessOrVisitor() throws Exception {
        var envelope = (com.fasterxml.jackson.databind.node.ObjectNode) RealtimeJob.JSON.readTree(fixture);
        var event = (com.fasterxml.jackson.databind.node.ObjectNode) envelope.path("event");
        event.remove("path"); event.put("app", "project_snow"); event.put("event_type", "request_complete");
        event.put("request_id", "request_1"); event.put("character_id", "snow"); event.put("elapsed_ms", 50);
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(envelope.toString(), "synthetic"));
        event.put("success", true);
        assertEquals("request:request_1", RealtimeJob.normalize(envelope.toString(), "synthetic").path("business_key").asText());
        event.put("anonymous_id", "00000000-0000-4000-8000-000000000001");
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(envelope.toString(), "synthetic"));
        event.remove("anonymous_id"); event.put("success", "true");
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(envelope.toString(), "synthetic"));
    }
    @Test void latenessBoundaryAndInitialWatermarkDoNotOverflow() {
        assertFalse(RealtimeJob.tooLate(0, Long.MIN_VALUE));
        assertFalse(RealtimeJob.tooLate(600_000, 1_200_000));
        assertTrue(RealtimeJob.tooLate(599_999, 1_200_000));
    }
    @Test void canonicalFingerprintIgnoresOrderAndOptionalNulls() throws Exception {
        var event = (com.fasterxml.jackson.databind.node.ObjectNode) RealtimeJob.JSON.readTree(fixture).path("event");
        event.putNull("session_id");
        var other = (com.fasterxml.jackson.databind.node.ObjectNode) RealtimeJob.JSON.readTree(fixture);
        other.set("event", event); other.put("seq", 2);
        assertEquals(RealtimeJob.normalize(fixture, "synthetic").path("_event_json"), RealtimeJob.normalize(other.toString(), "synthetic").path("_event_json"));
        assertTrue(RealtimeJob.normalize(fixture, "synthetic").path("business_version").asLong() > RealtimeJob.normalize(other.toString(), "synthetic").path("business_version").asLong());
    }
}
