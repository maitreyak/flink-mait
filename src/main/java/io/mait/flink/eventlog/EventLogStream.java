package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;


@Slf4j
public class EventLogStream {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("s3.endpoint", "http://127.0.0.1:9000");
        configuration.setString("s3.access-key", "minioadmin");
        configuration.setString("s3.secret-key", "minioadmin");
        FileSystem.initialize(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("s3://tmp/checkpoint-dir");
        DataStream<GenericRecord> eventLogDataStream = env.addSource(new EventLogSource());
        final FileSink<GenericRecord> psink = FileSink
                .forBulkFormat(new Path("s3://tmp/something"), AvroParquetWriters.forGenericRecord(EventLog.getClassSchema()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        eventLogDataStream.sinkTo(psink);
        env.execute("File writer");

    }
}