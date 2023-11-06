package kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.mait.flink.avro.BizEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;

@Slf4j
public class GenericRecordKafkaConsumer {

    final static SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 10);
    final static KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);

    final static KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);


    public static void main(String[] args) throws Exception {
        schemaRegistryClient.register("sink-biz-value", BizEvent.SCHEMA$);

        KafkaSource<GenericRecord> kafkaSource = KafkaSource
                .<GenericRecord>builder()
                .setTopics("biz")
                .setBootstrapServers("127.0.0.1:19092")
                .setGroupId(UUID.randomUUID().toString())
                .setDeserializer(new KafkaRecordDeserializationSchema<GenericRecord>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<GenericRecord> collector) throws IOException {
                        collector.collect((GenericRecord)
                                kafkaAvroDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value(), BizEvent.SCHEMA$));
                    }

                    @Override
                    public TypeInformation<GenericRecord> getProducedType() {
                        return new GenericRecordAvroTypeInfo(BizEvent.SCHEMA$);
                    }
                }).build();

        final KafkaSink<GenericRecord> kafkaSink = KafkaSink
                .<GenericRecord>builder().setBootstrapServers("127.0.0.1:19092")
                        .setRecordSerializer(new KafkaRecordSerializationSchema<GenericRecord>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(GenericRecord bizEvent, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                log.info("Events and stuff {}", bizEvent);
                                return new ProducerRecord<>("sink-biz", kafkaAvroSerializer.serialize("sink-biz", bizEvent));

                            }}
                        ).build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<GenericRecord> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        dataStream.sinkTo(kafkaSink);
        env.execute("full circle");
    }
}
