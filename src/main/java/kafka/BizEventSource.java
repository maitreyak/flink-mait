package kafka;

import io.mait.flink.avro.BizEvent;
import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

@Slf4j
public class BizEventSource extends RichParallelSourceFunction <BizEvent> {

    Boolean running;
    public BizEventSource() {
        running = true;
    }

    @Override
    public void run(SourceContext<BizEvent> sourceContext) throws Exception {
        while(running) {
            for(int i=0; i< 100; i++) {
                BizEvent bizEvent = BizEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setEventTimestamp(System.currentTimeMillis())
                        .build();
                sourceContext.collect(bizEvent);
            }
            Thread.sleep(100);
        }
    }
    @Override
    public void cancel() {
        this.running = false;
    }
}
