import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WeatherEvent {
    public String id;
    public String event;
    public String severity;
    public String urgency;
    public String areas;
    public String onset;
    public String expires;

    public WeatherEvent() {}
}