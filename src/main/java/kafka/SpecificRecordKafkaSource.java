package kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.mait.flink.avro.BizEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class SpecificRecordKafkaSource {
        final static SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://127.0.0.1:8081", 10);
        final static KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);

    public static void main(String[] args) throws Exception {
        schemaRegistryClient.register("biz-value", BizEvent.SCHEMA$);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        DataStreamSource<BizEvent> dataStream = env.addSource(new BizEventSource());
        final KafkaSink<BizEvent> kafkaSink =
                KafkaSink.<BizEvent>builder().setBootstrapServers("127.0.0.1:19092")
                        .setRecordSerializer(new KafkaRecordSerializationSchema<BizEvent>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(BizEvent bizEvent, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                return new ProducerRecord<>("biz", kafkaAvroSerializer.serialize("biz",bizEvent));
                            }
                        }).build();

        dataStream.sinkTo(kafkaSink);
        env.execute("kafka specific record produce");
    }

}
