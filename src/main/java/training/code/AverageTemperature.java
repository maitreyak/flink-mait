package training.code;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import training.models.SensorReading;

import java.util.Random;

public class AverageTemperature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> sensorReadingDataStream = env.addSource(new SensorReadingSource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sensorReadingDataStream
                .assignTimestampsAndWatermarks(new SensorTimeAssigner())
                .keyBy(it->it.getId())
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out){
                        // compute the average temperature
                        int cnt = 0;
                        double sum = 0.0;
                        String id = null;
                        for (SensorReading r : input) {
                            cnt++;
                            sum += r.getTemperature();
                            id = r.getId();
                        }
                        double avgTemp = sum / cnt;

                        // emit a SensorReading with the average temperature
                        out.collect(SensorReading.builder().id(id).timestamp(window.getEnd()).temperature(avgTemp).build());
                    }
                }).sinkTo(new PrintSink<>());
        env.execute("avg temperature");
    }
}

class SensorReadingSource extends RichParallelSourceFunction<SensorReading> {

    Boolean running;

    public SensorReadingSource() {
        running = true;
    }

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random();
        while (running) {

            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 100; j++) {
                    sourceContext.collect(SensorReading
                            .builder()
                            .id("id:"+i)
                            .timestamp(System.currentTimeMillis())
                            .temperature(random.nextDouble())
                            .build());
                    Thread.sleep(100);
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}

class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    /**
     * Configures the extractor with 5 seconds out-of-order interval.
     */
    public SensorTimeAssigner() {
        super(Time.seconds(5));
    }

    /**
     * Extracts timestamp from SensorReading.
     *
     * @param r sensor reading
     * @return the timestamp of the sensor reading.
     */
    @Override
    public long extractTimestamp(SensorReading r) {
        return r.getTimestamp();
    }
}
