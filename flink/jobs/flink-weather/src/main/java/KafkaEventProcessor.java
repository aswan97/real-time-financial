import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KafkaEventProcessor {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("polling-weather")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create the raw string stream from Kafka
        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // Parse JSON into WeatherEvent objects
        DataStream<WeatherEvent> parsedStream = stream 
            .flatMap(new WeatherEventParser())
            .returns(WeatherEvent.class);

        // Format all events as a string for the sink
        DataStream<String> outputStream = parsedStream
            .map(event -> "{" + "'id':" + "'" + event.id + "'" + "," 
                + " 'event':" + "'" + event.event + "'" + ","
                + " 'severity':" + "'" + event.severity + "'" + ","
                + " 'urgency':" + "'" + event.urgency + "'" + ","
                + " 'areas':" + "'" + event.areas + "'" + ","
                + " 'onset':" + "'" + event.onset + "'" + ","
                + " 'expires':" + "'" + event.expires + "'" + "}");

        // Count events per severity in 1-minute tumbling windows
        DataStream<Tuple2<String, Integer>> countStream = parsedStream
            .map(event -> new Tuple2<>(event.severity, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .sum(1);

        // Print windowed counts to stdout
        countStream.print();

        // Sink all events to "processed-events"
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

        outputStream.sinkTo(sink);

        // Actually executing the job
        env.execute("Kafka Event Processor");
    }
}