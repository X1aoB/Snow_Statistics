package dev.xiaob.snow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** The accepted v1 event shape, independently checked at the laboratory boundary. */
final class EventContract {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> BASE = Set.of("schema_version", "event_id", "app", "event_type", "occurred_at");
    private static final Map<String, Set<String>> REQUIRED = Map.of(
        "page_view", Set.of("path"), "character_select", Set.of("character_id"),
        "entry_click", Set.of("jump_id", "channel"), "entry_arrival", Set.of("jump_id"),
        "request_observed", Set.of("request_id"),
        "request_complete", Set.of("request_id", "character_id", "success", "elapsed_ms"));

    static ObjectNode validate(JsonNode envelope, String source) throws Exception {
        if (!envelope.isObject() || !Set.of("real", "synthetic").contains(source)
                || !source.equals(envelope.path("source").asText())
                || !envelope.path("seq").isIntegralNumber() || !envelope.path("seq").canConvertToLong()
                || envelope.path("seq").asLong() <= 0) throw new IllegalArgumentException("provenance");
        instant(envelope.path("accepted_at").asText());
        JsonNode input = envelope.path("event");
        if (!input.isObject() || !input.path("schema_version").isIntegralNumber()
                || !input.path("schema_version").canConvertToInt()
                || input.path("schema_version").asInt() != 1) throw new IllegalArgumentException("version");
        String kind = input.path("event_type").asText(), app = input.path("app").asText();
        if (!REQUIRED.containsKey(kind) || !Set.of("mywebsite", "project_snow").contains(app))
            throw new IllegalArgumentException("event kind");
        if (!kind.equals("page_view") && !app.equals(kind.equals("entry_click") ? "mywebsite" : "project_snow"))
            throw new IllegalArgumentException("event app");
        Set<String> allowed = new HashSet<>(BASE);
        allowed.addAll(REQUIRED.get(kind));
        if (!kind.equals("request_complete")) allowed.addAll(Set.of("anonymous_id", "session_id"));
        // Null optional fields are permitted by Event.model_dump; non-null fields must match the shape.
        Set<String> known = new HashSet<>(BASE);
        REQUIRED.values().forEach(known::addAll);
        known.addAll(Set.of("anonymous_id", "session_id"));
        ObjectNode event = ((ObjectNode) input).deepCopy();
        for (Iterator<Map.Entry<String, JsonNode>> it = event.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> field = it.next();
            if (!known.contains(field.getKey())) throw new IllegalArgumentException("unknown field");
            if (field.getValue().isNull()) it.remove();
            else if (!allowed.contains(field.getKey())) throw new IllegalArgumentException("event shape");
        }
        for (String field : BASE) if (!event.has(field)) throw new IllegalArgumentException("missing base");
        for (String field : REQUIRED.get(kind)) if (!event.has(field)) throw new IllegalArgumentException("missing field");
        for (String field : Set.of("event_id", "anonymous_id", "session_id", "jump_id")) {
            if (event.has(field)) {
                String id = text(event, field);
                if (!id.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
                    throw new IllegalArgumentException("UUID");
                event.put(field, UUID.fromString(id).toString());
            }
        }
        for (String field : Set.of("character_id", "channel", "request_id"))
            if (event.has(field) && !text(event, field).matches("[A-Za-z0-9_-]{1,64}")) throw new IllegalArgumentException("identifier");
        event.put("occurred_at", instant(text(event, "occurred_at")).toString());
        if (event.has("path")) {
            String path = text(event, "path");
            if (path.length() > 200 || !path.startsWith("/") || path.contains("?") || path.contains("#"))
                throw new IllegalArgumentException("page path");
        }
        if (kind.equals("request_complete") && (!event.path("success").isBoolean()
                || !event.path("elapsed_ms").isIntegralNumber() || !event.path("elapsed_ms").canConvertToLong()
                || event.path("elapsed_ms").asLong() < 0 || event.path("elapsed_ms").asLong() > 86_400_000))
            throw new IllegalArgumentException("completion fields");
        // Field order cannot turn a byte-identical business event into a conflict.
        TreeMap<String, JsonNode> sorted = new TreeMap<>();
        event.fields().forEachRemaining(f -> sorted.put(f.getKey(), f.getValue()));
        return (ObjectNode) JSON.valueToTree(sorted);
    }

    private static String text(JsonNode event, String field) {
        if (!event.path(field).isTextual()) throw new IllegalArgumentException("text field");
        return event.path(field).asText();
    }
    static Instant instant(String value) {
        // Java 11 Instant.parse accepts Z but rejects offsets accepted by newer JDKs.
        return OffsetDateTime.parse(value).toInstant();
    }
}
