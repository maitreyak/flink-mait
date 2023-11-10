package com.event.tracking;

import io.mait.flink.avro.EventLog;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.UUID;
import java.util.function.Predicate;

public class EventTracking {
    public static void main(String[] args) {

    }
}

class FilterProcessFunction extends ProcessFunction<GenericRecord, GenericRecord> {

    Predicate<GenericRecord> predicate;
    OutputTag<GenericRecord> eventLogOutputs;

    FilterProcessFunction(final Predicate<GenericRecord> predicate, final OutputTag<GenericRecord> eventLogOutputs) {
        this.predicate = predicate;
        this.eventLogOutputs = eventLogOutputs;
    }

    @Override
    public void processElement(GenericRecord value, ProcessFunction<GenericRecord, GenericRecord>.Context ctx, Collector<GenericRecord> out) {
        if (predicate.test(value)) {
            out.collect(value);
            return;
        }
        ctx.output(eventLogOutputs, EventLog.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventTimestamp(System.currentTimeMillis())
                .build());
    }
}



