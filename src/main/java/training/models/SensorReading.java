package training.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SensorReading {
    String id;
    Long timestamp;
    Double temperature;
}
