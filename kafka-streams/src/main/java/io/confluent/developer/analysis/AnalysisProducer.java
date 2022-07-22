package io.confluent.developer.analysis;

import io.confluent.developer.StreamsUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class AnalysisProducer implements Runnable {
    private final Properties properties;

    private final KafkaProducer<String, String> producer;
    private boolean shutdown = false;

    public AnalysisProducer() throws IOException {
        properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "AnalysisProducer");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");

        createInputTopic();
        producer = new KafkaProducer<>(properties);
        System.out.println("...init AnalysisProducer OK");
    }

    private void createInputTopic() {
        try(Admin adminClient = Admin.create(properties)) {
            var topics = List.of(StreamsUtils.createTopic(AnalysisExample.INPUT_TOPIC));
            adminClient.createTopics(topics);
            System.out.println("...created topic if needed");
        }
    }

    @Override
    public void run() {
        System.out.println("...entering main loop");
        if (! shutdown) {
            System.out.println("...not shutdown, proceeding");
            String jsonKey = UUID.randomUUID().toString();
            String jsonValue = "{\"hello\": \"world\"}";

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(AnalysisExample.INPUT_TOPIC, jsonKey, jsonValue);
            try {
                Future<RecordMetadata> futureRecordMetadata = producer.send(record);
                System.out.println("...record sent to kafka");

                RecordMetadata recordMetadata = futureRecordMetadata.get();
                System.out.println("...record received by kafka: " + recordMetadata.toString());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
