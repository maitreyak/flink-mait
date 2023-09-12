package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

@Slf4j
public class EventLogTupleSource extends RichParallelSourceFunction <Tuple2<String, String>> {

    Boolean running;
    public EventLogTupleSource() {
        running = true;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        while(running) {
            for(int i=0; i< 100; i++) {
                UUID uuid = UUID.randomUUID();
                Tuple2<String, String> eventLog = Tuple2.of(UUID.randomUUID().toString(), String.format("%s:%s","dingbat", uuid));
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
