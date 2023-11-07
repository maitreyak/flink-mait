package kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.mait.flink.avro.BizEvent;
import io.mait.flink.avro.SuperBizEvent;
import io.openlineage.flink.OpenLineageFlinkJobListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

@Slf4j
public class GenericRecordConsumerWithLineage {

    final static SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 10);

    public static void main(String[] args) throws Exception {
        //Set environment variable
        //OPENLINEAGE_CONFIG=/src/main/resources/openlineage.yml
        schemaRegistryClient.register("sink-super-biz-value", SuperBizEvent.SCHEMA$);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = (Configuration) env.getConfiguration();
        configuration.setString("rest.address", "localhost");
        configuration.setString("rest.port", "9000");

        final KafkaSource<GenericRecord> kafkaSource = KafkaSource
                .<GenericRecord>builder()
                .setTopics("biz")
                .setBootstrapServers("127.0.0.1:19092")
                .setGroupId(UUID.randomUUID().toString())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(BizEvent.SCHEMA$, "http://127.0.0.1:8081"))
                .build();

        final KafkaSink<GenericRecord> kafkaSink = KafkaSink
                .<GenericRecord>builder()
                .setBootstrapServers("127.0.0.1:19092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(
                                ConfluentRegistryAvroSerializationSchema
                                        .forGeneric("sink-super-biz", SuperBizEvent.SCHEMA$, "http://127.0.0.1:8081"))
                        .setTopic("sink-super-biz")
                        .build()
                ).build();
        
        env.enableCheckpointing(1000);

        DataStreamSource<GenericRecord> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        dataStream.process(new ProcessFunction<GenericRecord, GenericRecord>() {
                    @Override
                    public void processElement(GenericRecord value, ProcessFunction<GenericRecord, GenericRecord>.Context ctx, Collector<GenericRecord> out) throws Exception {
                        log.info("Input {}", value);
                        SuperBizEvent superBizEvent = SuperBizEvent.newBuilder()
                                .setEventEventId(UUID.randomUUID().toString())
                                .setEventId(UUID.randomUUID().toString())
                                .setEventTimestamp(System.currentTimeMillis())
                                .setEventPublishTimestamp(System.currentTimeMillis())
                                .build();
                        log.info("Output {}", superBizEvent);
                        out.collect(superBizEvent);
                    }
                })
                .sinkTo(kafkaSink);

        String jobName = String.format("ohmyjob", UUID.randomUUID());
        JobListener jobListener = OpenLineageFlinkJobListener.builder()
                .executionEnvironment(env)
                .jobName(jobName)
                .build();

        env.registerJobListener(jobListener);
        env.execute(jobName);
    }
}
