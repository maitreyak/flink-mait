package io.mait.flink.eventlog;

import io.mait.flink.avro.EventLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class EventLogSideOutputs {
    public static void main(String[] args) throws Exception {
        setupFileSystem();  //for object store to work from local
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
            Setup the environment
        */
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/checkpoint-dir");

        final FileSink<GenericRecord> rawSink1 = fileSinkAvroParquetBulk("file:///tmp/raw-1");
        final FileSink<GenericRecord> validatedSink1 = fileSinkAvroParquetBulk("file:///tmp/validated-1");

        final OutputTag<GenericRecord> rawZoneSource = new OutputTag<>("rawZoneSource", TypeInformation.of(GenericRecord.class));
        final OutputTag<String> eventLogString = new OutputTag<>("eventLogString", TypeInformation.of(String.class));

        DataStream<GenericRecord> mainStream = env.addSource(new EventLogSource());
        SingleOutputStreamOperator<GenericRecord> mainStreamOperator= mainStream.process(new ProcessFunction<GenericRecord, GenericRecord>() {
            @Override
            public void processElement(GenericRecord value, ProcessFunction<GenericRecord, GenericRecord>.Context ctx, Collector<GenericRecord> out) throws Exception {

                out.collect(value);
                ctx.output(rawZoneSource, value);
                ctx.output(eventLogString, value.toString());
            }
        });

        DataStream<GenericRecord> rawStream = mainStreamOperator.getSideOutput(rawZoneSource);
        DataStream<String> eventLogStringSteam = mainStreamOperator.getSideOutput(eventLogString);

        mainStreamOperator.sinkTo(validatedSink1);
        rawStream.sinkTo(rawSink1);
        eventLogStringSteam.sinkTo(new PrintSink());

        env.execute("File writer");
    }

    static void setupFileSystem() {
        Configuration configuration = new Configuration();
        configuration.setString("s3.endpoint", "http://127.0.0.1:9000");
        configuration.setString("s3.access-key", "minioadmin");
        configuration.setString("s3.secret-key", "minioadmin");
        FileSystem.initialize(configuration);
    }

    static FileSink<GenericRecord> fileSinkAvroParquetBulk(final String path) {
        return FileSink
                .forBulkFormat(new Path(path), AvroParquetWriters.forGenericRecord(EventLog.getClassSchema()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }
}