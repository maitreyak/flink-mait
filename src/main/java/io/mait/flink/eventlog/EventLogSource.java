package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.util.UUID;
@Slf4j
public class EventLogSource extends RichParallelSourceFunction <GenericRecord> {

    Boolean running;
    public EventLogSource() {
        running = true;
    }

    @Override
    public void run(SourceContext<GenericRecord> sourceContext) throws Exception {
        while(running) {
            for(int i=0; i< 100; i++) {
                EventLog eventLog = EventLog.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setEventTimestamp(System.currentTimeMillis())
                        .build();
                sourceContext.collect(eventLog);
            }
            Thread.sleep(100);
        }
    }
    @Override
    public void cancel() {
        this.running = false;
    }
}
