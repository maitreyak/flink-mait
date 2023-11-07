package kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.mait.flink.avro.BizEvent;
import io.openlineage.flink.OpenLineageFlinkJobListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

@Slf4j
public class GenericRecordConsumerWithLineage {

    final static SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 10);

    public static void main(String[] args) throws Exception {
        schemaRegistryClient.register("sink-biz-value", BizEvent.SCHEMA$);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = (Configuration) env.getConfiguration();
        configuration.setString("rest.address", "localhost");
        configuration.setString("rest.port", "9000");

        final KafkaSource<GenericRecord> kafkaSource = KafkaSource
                .<GenericRecord>builder()
                .setTopics("biz")
                .setBootstrapServers("127.0.0.1:19092")
                .setGroupId(UUID.randomUUID().toString())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(BizEvent.SCHEMA$,"http://127.0.0.1:8081"))
                .build();

        final KafkaSink<GenericRecord> kafkaSink = KafkaSink
                .<GenericRecord>builder()
                .setBootstrapServers("127.0.0.1:19092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema
                        .forGeneric("sink-biz", BizEvent.SCHEMA$,  "http://127.0.0.1:8081"))
                        .setTopic("sink-biz")
                        .build()
                ).build();



        env.enableCheckpointing(1000);

        DataStreamSource<GenericRecord> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        dataStream.sinkTo(kafkaSink);
        JobListener jobListener = OpenLineageFlinkJobListener.builder()
                .executionEnvironment(env)
                .jobName("test-crap")
                .build();

        env.registerJobListener(jobListener);
        env.execute("test-crap");

    }
}
