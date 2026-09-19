package dev.xiaob.snow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.Set;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RealtimeJob {
    static final ObjectMapper JSON = new ObjectMapper();
    static final OutputTag<String> LATE = new OutputTag<String>("late-for-offline") {};
    static final OutputTag<String> INVALID = new OutputTag<String>("invalid-contract") {};
    static final OutputTag<String> DUPLICATE = new OutputTag<String>("duplicate") {};
    static final Set<String> KINDS = Set.of("page_view", "character_select", "entry_click", "entry_arrival", "request_observed", "request_complete");

    public static ObjectNode normalize(String value, String expectedSource) throws Exception {
        if (value.length() > 65536) throw new IllegalArgumentException("oversized record");
        JsonNode envelope = JSON.readTree(value), event = EventContract.validate(envelope, expectedSource);
        Instant time = EventContract.instant(event.path("occurred_at").asText());
        Instant accepted = EventContract.instant(envelope.path("accepted_at").asText());
        String kind = event.path("event_type").asText();
        String identity = event.path(kind.equals("request_complete") ? "request_id" : "event_id").asText();
        if (identity.isBlank()) throw new IllegalArgumentException("missing business identity");
        ObjectNode out = JSON.createObjectNode();
        out.put("source", expectedSource); out.put("app", event.path("app").asText());
        out.put("business_key", (kind.equals("request_complete") ? "request:" : "event:") + identity);
        out.put("event_type", kind); out.put("business_date", time.atZone(ZoneId.of("Asia/Hong_Kong")).toLocalDate().toString());
        for (String field : new String[]{"anonymous_id", "character_id"}) {
            if (event.hasNonNull(field)) out.put(field, event.path(field).asText()); else out.putNull(field);
        }
        if (event.hasNonNull("path")) out.put("page", event.path("path").asText()); else out.putNull("page");
        if (kind.equals("request_complete")) out.put("success", event.path("success").asBoolean()); else out.putNull("success");
        out.put("event_time", time.toString().replace("T", " ").replace("Z", ""));
        out.put("accepted_at", accepted.toString().replace("T", " ").replace("Z", ""));
        // Immutable event fact: first acceptance wins. Dedup prevents later source
        // sequence values from making the same request count under another day.
        // Immutable facts use the earliest source sequence even after state TTL expires.
        // A lane belongs to one collector sequence generation; never reset it in place.
        out.put("business_version", Long.MAX_VALUE - envelope.path("seq").asLong());
        out.put("_event_id", event.path("event_id").asText());
        // Real dedup state needs a fingerprint, not a second complete event copy.
        // Synthetic v2 stays compatible with its retained experimental checkpoints.
        out.put("_event_json", expectedSource.equals("real") ? fingerprint(event.toString()) : event.toString());
        ObjectNode replay = JSON.createObjectNode();
        replay.put("source", expectedSource); replay.put("seq", envelope.path("seq").asLong());
        replay.put("accepted_at", accepted.toString()); replay.set("event", event);
        out.set("_envelope", replay);
        return out;
    }

    static String fingerprint(String value) throws Exception {
        byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder();
        for (byte item : bytes) hex.append(String.format("%02x", item & 0xff));
        return hex.toString();
    }

    static void realWindow(Instant accepted, Instant readableFrom, Instant notAfter, Instant now) {
        if (!now.isBefore(notAfter) || notAfter.isAfter(readableFrom.plus(Duration.ofDays(7))))
            throw new IllegalStateException("Real cleanup/restore window expired");
        if (accepted.isBefore(readableFrom) || accepted.isAfter(now) || !accepted.plus(Duration.ofDays(7)).isAfter(now))
            throw new IllegalStateException("Real event is outside registered retention window");
    }

    static void realEpoch(String epoch, String generation, Instant epochFrom, Instant epochUntil,
                          Instant readableFrom, Instant notAfter, String lane) {
        if (!epoch.matches("[a-z][a-z0-9-]{2,23}") || !lane.equals(epoch.replace('-', '_')) ||
            !java.util.UUID.fromString(generation).toString().equals(generation) ||
            !epochUntil.equals(epochFrom.plus(Duration.ofDays(7))) ||
            !readableFrom.equals(epochFrom) || !notAfter.equals(epochUntil))
            throw new IllegalArgumentException("Real job differs from frozen storage epoch");
    }

    static boolean tooLate(long timestamp, long watermark) {
        return watermark != Long.MIN_VALUE && timestamp < watermark - 600_000L;
    }

    static String dorisRow(String value) throws Exception {
        ObjectNode row = (ObjectNode) JSON.readTree(value);
        row.remove(java.util.List.of("_event_id", "_event_json", "_envelope"));
        return row.toString();
    }

    static String diagnostic(String value, String reason) throws Exception {
        JsonNode row = JSON.readTree(value);
        ObjectNode result = JSON.createObjectNode();
        result.put("reason", reason); result.put("seq", row.path("_envelope").path("seq").asLong());
        result.put("event_id", row.path("_event_id").asText());
        if (row.path("_envelope").path("source").asText().equals("real")) {
            result.put("source", "real");
            result.put("accepted_at", row.path("_envelope").path("accepted_at").asText());
        }
        return result.toString();
    }

    static class EventDeduplicate extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<String> seen;
        private final boolean real;
        EventDeduplicate(boolean real) { this.real = real; }
        @Override public void open(Configuration parameters) {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(real ? "event-sha256-real-v1" : "event-fingerprint-v2", String.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(real ? 30 : 8)).build());
            seen = getRuntimeContext().getState(descriptor);
        }
        @Override public void processElement(String value, Context context, Collector<String> out) throws Exception {
            String fingerprint = JSON.readTree(value).path("_event_json").asText();
            if (seen.value() != null) {
                boolean same = seen.value().equals(fingerprint);
                context.output(same ? DUPLICATE : INVALID, diagnostic(value, same ? "event_duplicate" : "event_id_conflict"));
            } else if (tooLate(context.timestamp(), context.timerService().currentWatermark())) {
                context.output(LATE, JSON.readTree(value).path("_envelope").toString());
            } else { seen.update(fingerprint); out.collect(value); }
        }
    }

    static class RequestDeduplicate extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Boolean> seen;
        private final boolean real;
        RequestDeduplicate(boolean real) { this.real = real; }
        @Override public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-v1", Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(real ? 30 : 8)).build());
            seen = getRuntimeContext().getState(descriptor);
        }
        @Override public void processElement(String value, Context context, Collector<String> out) throws Exception {
            if (seen.value() == null) { seen.update(true); out.collect(value); }
            else context.output(DUPLICATE, diagnostic(value, "request_duplicate"));
        }
    }

    static class Parse extends ProcessFunction<String, String> {
        private final String source;
        private final Instant readableFrom, notAfter;
        Parse(String source, Instant readableFrom, Instant notAfter) {
            this.source = source; this.readableFrom = readableFrom; this.notAfter = notAfter;
        }
        @Override public void processElement(String value, Context context, Collector<String> out) {
            ObjectNode normalized;
            try { normalized = normalize(value, source); }
            catch (Exception error) {
                System.err.println("snow_realtime_rejected class=" + error.getClass().getSimpleName());
                context.output(INVALID, "{\"reason\":\"invalid_contract\"}");
                return;
            }
            // Fail the real job, rather than sending an expired payload to another topic.
            if (source.equals("real")) realWindow(Instant.parse(normalized.path("_envelope").path("accepted_at").asText()),
                                                  readableFrom, notAfter, Instant.now());
            out.collect(normalized.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = required("KAFKA_BOOTSTRAP"), sourceName = required("SNOW_SOURCE");
        if (!Set.of("real", "synthetic").contains(sourceName)) throw new IllegalArgumentException("source");
        boolean real = sourceName.equals("real");
        Instant readableFrom = real ? EventContract.instant(required("SNOW_REAL_READABLE_FROM")) : null;
        Instant notAfter = real ? EventContract.instant(required("SNOW_REAL_RESTORE_NOT_AFTER")) : null;
        if (real) realWindow(readableFrom, readableFrom, notAfter, Instant.now());
        String lane = System.getenv().getOrDefault("SNOW_REPLAY_LANE", "live");
        if (!lane.matches("[a-z0-9_-]{1,32}")) throw new IllegalArgumentException("lane");
        if (real) realEpoch(required("SNOW_REAL_EPOCH_ID"), required("SNOW_REAL_EPOCH_GENERATION"),
                            EventContract.instant(required("SNOW_REAL_EPOCH_FROM")), EventContract.instant(required("SNOW_REAL_EPOCH_UNTIL")),
                            readableFrom, notAfter, lane);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(5)));
        String topic = System.getenv().getOrDefault("SNOW_INPUT_TOPIC", "snow." + sourceName + ".events.v1");
        if (!topic.matches("snow\\." + sourceName + "\\.[a-z0-9_.-]{1,100}\\.v1")) throw new IllegalArgumentException("topic/source");
        if (real && !topic.equals("snow.real." + lane + ".events.v1"))
            throw new IllegalArgumentException("Real topic is outside its frozen epoch lane");
        // First acceptance ordering is validated for one collector/partition in this release.
        Properties adminProperties = new Properties();
        adminProperties.put("bootstrap.servers", bootstrap);
        adminProperties.put("default.api.timeout.ms", "10000");
        adminProperties.put("request.timeout.ms", "5000");
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(adminProperties)) {
            var description = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get().get(topic);
            if (description.partitions().size() != 1)
                throw new IllegalArgumentException("Realtime v2 requires one ordered input partition");
            if (real && (!description.topicId().toString().equals(required("SNOW_REAL_TOPIC_ID")) ||
                         !admin.describeCluster().clusterId().get().equals(required("SNOW_REAL_CLUSTER_ID"))))
                throw new IllegalArgumentException("Real Kafka generation differs from registered input");
        }
        String sidePrefix = "snow." + sourceName + "." + lane;
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(bootstrap)
            .setTopics(topic).setGroupId("snow-flink-" + sourceName + "-" + lane)
            .setStartingOffsets(real ? OffsetsInitializer.offsets(java.util.Map.of(
                new org.apache.kafka.common.TopicPartition(topic, 0), Long.parseLong(required("SNOW_REAL_START_OFFSET"))),
                org.apache.kafka.clients.consumer.OffsetResetStrategy.NONE) :
                OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var parsed = env.fromSource(source, WatermarkStrategy.noWatermarks(), "accepted-events-v1")
            .uid("source-v2")
            .process(new Parse(sourceName, readableFrom, notAfter)).uid("parse-v1");
        var events = parsed
            .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((value, previous) -> {
                    try { return Instant.parse(JSON.readTree(value).path("event_time").asText().replace(" ", "T") + "Z").toEpochMilli(); }
                    catch (Exception error) { throw new IllegalArgumentException("timestamp"); }
                }).withIdleness(Duration.ofMinutes(1)))
            .keyBy(value -> { JsonNode row = JSON.readTree(value); return row.path("source").asText()+":"+row.path("app").asText()+":"+row.path("_event_id").asText(); })
            .process(new EventDeduplicate(real)).uid("event-dedup-v2");
        var stream = events.keyBy(value -> { JsonNode row = JSON.readTree(value); return row.path("source").asText()+":"+row.path("app").asText()+":"+row.path("business_key").asText(); })
            .process(new RequestDeduplicate(real)).uid("request-dedup-v2");
        parsed.getSideOutput(INVALID).union(events.getSideOutput(INVALID))
            .sinkTo(kafkaSink(bootstrap, sidePrefix + ".quarantine.v1")).uid("quarantine-v2");
        events.getSideOutput(DUPLICATE).union(stream.getSideOutput(DUPLICATE))
            .sinkTo(kafkaSink(bootstrap, sidePrefix + ".duplicates.v1")).uid("duplicates-v2");
        String table = System.getenv().getOrDefault("DORIS_TABLE", "snow_realtime_v2.events_realtime");
        if (!table.matches("snow(?:_[a-z0-9_]{1,40})?\\.events_realtime")) throw new IllegalArgumentException("table");
        if (real && !table.matches("snow_real_[a-z0-9_]{1,30}\\.events_realtime"))
            throw new IllegalArgumentException("Real output requires an independently permissioned snow_real_* database");
        if (real && !table.equals("snow_real_" + lane + ".events_realtime"))
            throw new IllegalArgumentException("Real output is outside its frozen epoch lane");
        Properties props = new Properties(); props.setProperty("format", "json"); props.setProperty("read_json_by_line", "true");
        DorisSink<String> sink = DorisSink.<String>builder()
            .setDorisOptions(DorisOptions.builder().setFenodes(required("DORIS_FE"))
                .setTableIdentifier(table).setUsername(required("DORIS_USER")).setPassword(System.getenv().getOrDefault("DORIS_PASSWORD", "")).build())
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder().setLabelPrefix("snow-"+sourceName+"-"+lane).setStreamLoadProp(props).setDeletable(false).build())
            .setSerializer(new SimpleStringSerializer()).build();
        stream.map(RealtimeJob::dorisRow).uid("doris-row-v2").sinkTo(sink).uid("doris-v2");
        events.getSideOutput(LATE).sinkTo(kafkaSink(bootstrap, sidePrefix + ".late.v1")).uid("late-v2");
        env.execute("Snow Statistics " + sourceName + " " + lane);
    }
    static KafkaSink<String> kafkaSink(String bootstrap, String topic) {
        return KafkaSink.<String>builder().setBootstrapServers(bootstrap)
            .setRecordSerializer((KafkaRecordSerializationSchema<String>) (value, context, timestamp) ->
                new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(topic, null,
                    originalKafkaTimestamp(value, timestamp), null, value.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }
    static Long originalKafkaTimestamp(String value, Long fallback) {
        try {
            JsonNode row = JSON.readTree(value);
            return row.path("source").asText().equals("real")
                ? EventContract.instant(row.path("accepted_at").asText()).toEpochMilli() : fallback;
        } catch (Exception error) { throw new IllegalArgumentException("Missing original real Kafka timestamp", error); }
    }
    static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
        return value;
    }
}
