package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.UUID;

@Slf4j
public class EventLogStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("s3://flink/checkpoint-dir");
        DataStream<GenericRecord> eventLogDataStream = env.addSource(new EventLogSource());
        final FileSink<GenericRecord> psink = FileSink
                .forBulkFormat(new Path("s3://flink/something"), AvroParquetWriters.forGenericRecord(EventLog.getClassSchema()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        UUID uuid = UUID.randomUUID();
        log.info("job name is {}", uuid);
        eventLogDataStream.sinkTo(psink).uid("sink_me_to_hell");
        env.execute("File writer");
    }
}
