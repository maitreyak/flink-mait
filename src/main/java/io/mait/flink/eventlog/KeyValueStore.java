package io.mait.flink.eventlog;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class KeyValueStore {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/checkpoint-dir");
        env.setParallelism(1);
        DataStreamSource<Tuple2<String,String>> streamSource =  env.addSource(new EventLogTupleSource());
        streamSource.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
                log.info("Map to {}", value);
                return value;
            }
        }).keyBy(value -> value.f0).asQueryableState("logs");
        env.execute("queryable state");
    }
}
