package dev.xiaob.snow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RealtimeJob {
    static final ObjectMapper JSON = new ObjectMapper();
    static final OutputTag<String> LATE = new OutputTag<String>("late-for-offline") {};
    static final OutputTag<String> INVALID = new OutputTag<String>("invalid-contract") {};
    static final Set<String> KINDS = Set.of("page_view", "character_select", "entry_click", "entry_arrival", "request_observed", "request_complete");

    public static ObjectNode normalize(String value, String expectedSource) throws Exception {
        JsonNode envelope = JSON.readTree(value), event = envelope.path("event");
        if (!Set.of("real", "synthetic").contains(expectedSource) || !expectedSource.equals(envelope.path("source").asText()) ||
            event.path("schema_version").asInt() != 1 || !KINDS.contains(event.path("event_type").asText()) ||
            !Set.of("mywebsite", "project_snow").contains(event.path("app").asText())) throw new IllegalArgumentException("invalid contract/provenance");
        Instant time = Instant.parse(event.path("occurred_at").asText());
        Instant accepted = Instant.parse(envelope.path("accepted_at").asText());
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
        out.put("business_version", envelope.path("seq").asLong());
        return out;
    }

    static class Deduplicate extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Boolean> seen;
        @Override public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-v1", Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(8)).build());
            seen = getRuntimeContext().getState(descriptor);
        }
        @Override public void processElement(String value, Context context, Collector<String> out) throws Exception {
            if (context.timestamp() < context.timerService().currentWatermark() - 600_000L) {
                context.output(LATE, value); return;
            }
            if (seen.value() == null) { seen.update(true); out.collect(value); }
        }
    }

    static class Parse extends ProcessFunction<String, String> {
        private final String source;
        Parse(String source) { this.source = source; }
        @Override public void processElement(String value, Context context, Collector<String> out) {
            try { out.collect(normalize(value, source).toString()); }
            catch (Exception error) { context.output(INVALID, "{\"reason\":\"invalid_contract\"}"); }
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = required("KAFKA_BOOTSTRAP"), sourceName = required("SNOW_SOURCE");
        if (!Set.of("real", "synthetic").contains(sourceName)) throw new IllegalArgumentException("source");
        String lane = System.getenv().getOrDefault("SNOW_REPLAY_LANE", "live");
        if (!lane.matches("[a-z0-9_-]{1,32}")) throw new IllegalArgumentException("lane");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); env.enableCheckpointing(10_000);
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(bootstrap)
            .setTopics("snow." + sourceName + ".events.v1").setGroupId("snow-flink-" + sourceName + "-" + lane)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var parsed = env.fromSource(source, WatermarkStrategy.noWatermarks(), "accepted-events-v1")
            .process(new Parse(sourceName)).uid("parse-v1");
        parsed.getSideOutput(INVALID).sinkTo(KafkaSink.<String>builder().setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("snow."+sourceName+".quarantine.v1")
                .setValueSerializationSchema(new SimpleStringSchema()).build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()).uid("quarantine-v1");
        var stream = parsed
            .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((value, previous) -> {
                    try { return Instant.parse(JSON.readTree(value).path("event_time").asText().replace(" ", "T") + "Z").toEpochMilli(); }
                    catch (Exception error) { throw new IllegalArgumentException("timestamp"); }
                }).withIdleness(Duration.ofMinutes(1)))
            .keyBy(value -> { JsonNode row = JSON.readTree(value); return row.path("source").asText()+":"+row.path("app").asText()+":"+row.path("business_key").asText(); })
            .process(new Deduplicate()).uid("dedup-v1");
        Properties props = new Properties(); props.setProperty("format", "json"); props.setProperty("read_json_by_line", "true");
        DorisSink<String> sink = DorisSink.<String>builder()
            .setDorisOptions(DorisOptions.builder().setFenodes(required("DORIS_FE"))
                .setTableIdentifier("snow.events_realtime").setUsername(required("DORIS_USER")).setPassword(required("DORIS_PASSWORD")).build())
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder().setLabelPrefix("snow-"+sourceName+"-"+lane).setStreamLoadProp(props).setDeletable(false).build())
            .setSerializer(new SimpleStringSerializer()).build();
        stream.sinkTo(sink).uid("doris-v1");
        stream.getSideOutput(LATE).sinkTo(KafkaSink.<String>builder().setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("snow."+sourceName+".late.v1")
                .setValueSerializationSchema(new SimpleStringSchema()).build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()).uid("late-v1");
        env.execute("Snow Statistics " + sourceName + " " + lane);
    }
    static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
        return value;
    }
}
