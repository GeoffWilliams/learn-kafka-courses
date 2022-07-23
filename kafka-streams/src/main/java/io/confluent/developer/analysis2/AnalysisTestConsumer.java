package io.confluent.developer.analysis2;

import io.confluent.developer.StreamsUtils;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AnalysisTestConsumer implements Runnable {
    private final Properties properties;
    private final KafkaConsumer<String,GenericRecord> consumer;
    private boolean shutdown = false;

    public AnalysisTestConsumer() throws IOException {
        properties = StreamsUtils.loadProperties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "AnalysisTestConsumer2");
        consumer = new KafkaConsumer<>(properties);
        System.out.println("...init AnalysisTestConsumer OK");
    }


    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(AnalysisExample.INPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (! shutdown) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(timeout);


            for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "key = %s, avro = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
        consumer.wakeup();
    }
}
