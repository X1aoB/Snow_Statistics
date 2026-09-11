package dev.xiaob.snow;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RealtimeJobTest {
    String fixture = "{\"source\":\"synthetic\",\"seq\":1,\"accepted_at\":\"2026-01-01T16:00:01Z\",\"event\":{\"schema_version\":1,\"event_id\":\"example\",\"app\":\"mywebsite\",\"event_type\":\"page_view\",\"occurred_at\":\"2026-01-01T16:00:00Z\",\"path\":\"/\",\"body\":\"never copy\"}}";
    @Test void timezoneAndAllowlist() throws Exception {
        var out = RealtimeJob.normalize(fixture, "synthetic");
        assertEquals("2026-01-02", out.path("business_date").asText());
        assertFalse(out.toString().contains("never copy"));
        assertEquals("event:example", out.path("business_key").asText());
    }
    @Test void provenanceCannotCross() {
        assertThrows(IllegalArgumentException.class, () -> RealtimeJob.normalize(fixture, "real"));
    }
}
