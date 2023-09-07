package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

public class EventLogStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/checkpoint-dir");
        DataStream<GenericRecord> eventLogDataStream = env.addSource(new EventLogSource());
        final FileSink<GenericRecord> psink = FileSink
                .forBulkFormat(new Path("file:///tmp/something"), AvroParquetWriters.forGenericRecord(EventLog.getClassSchema()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        eventLogDataStream.sinkTo(psink);

        env.execute("File writer");
    }
}
