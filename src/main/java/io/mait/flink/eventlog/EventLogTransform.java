package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;

@Slf4j
public class EventLogTransform {
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
                .forBulkFormat(new Path("s3://tmp/interim-something"), AvroParquetWriters.forGenericRecord(EventLog.getClassSchema()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        eventLogDataStream
                .transform("interim-s3", TypeInformation.of(CommittableMessage.class), new SinkWriterOperatorFactory(psink))
                .map(msg -> {
                    if(msg instanceof CommittableWithLineage)
                        log.info("{}",((CommittableWithLineage<FileSinkCommittable>) msg).getCommittable().getPendingFile().getPath());
                        return msg;
                })
                .transform("interim-committer", TypeInformation.of(String.class), new CommitterOperatorFactory(psink,false,true));

        env.execute("File writer");

    }
}