import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.Types;

public class KafkaEventProcessor {

    public static void main(String[] args) throws Exception {

        // 1. Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 10 seconds for fault tolerance
        //env.enableCheckpointing(10_000);

        // 2. Define the Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("polling-weather")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 3. Create the stream from the Kafka source
        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 4. Filter and transform: flag error events and forward to output topic
        DataStream<String> alertStream = stream
            .filter(event -> event.contains("ERROR"))
            .map(event -> "ALERT: " + event);

        // 5. Count all events per key using a 1-minute tumbling window
        DataStream<Tuple2<String, Integer>> countStream = stream
            .map(event -> new Tuple2<>(parseKey(event), 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .sum(1);

        // 6. Print windowed counts to stdout (useful for debugging)
        countStream.print();

        // 7. Define the Kafka sink and write alerts to "processed-events" topic
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("processed-events")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        alertStream.sinkTo(sink);

        // 8. Execute the job
        env.execute("Kafka Event Processor");
    }

    /**
     * Extracts a routing key from the raw event string.
     * Assumes events are formatted as "KEY:payload" (e.g. "user-service:ERROR something failed").
     * Falls back to "unknown" if no colon delimiter is found.
     */
    private static String parseKey(String event) {
        if (event == null || !event.contains(":")) {
            return "unknown";
        }
        return event.split(":")[0].trim();
    }
}