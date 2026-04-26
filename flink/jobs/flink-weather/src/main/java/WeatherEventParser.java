import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class WeatherEventParser extends RichFlatMapFunction<String, WeatherEvent> {

    private transient ObjectMapper mapper;  // transient = not serialized

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();        // instantiated on each worker
    }

    @Override
    public void flatMap(String json, Collector<WeatherEvent> out) {
        try {
            out.collect(mapper.readValue(json, WeatherEvent.class));
        } catch (Exception e) {
            // skip malformed messages
        }
    }
}