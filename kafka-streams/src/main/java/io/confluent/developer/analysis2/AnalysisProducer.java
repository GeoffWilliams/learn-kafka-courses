package io.confluent.developer.analysis2;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.developer.StreamsUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class AnalysisProducer implements Runnable {
    private final Properties properties;

    private final KafkaProducer<String, GenericRecord> producer;
    private boolean shutdown = false;
    private final String schemaString =
            "{\"namespace\": \"analytics.avro\"," +
                "\"type\": \"record\", " +
                "\"name\": \"Event\"," +
                "\"fields\": [" +
                    "{\"name\": \"created\", \"type\": [\"null\", \"long\"], \"logicalType\": \"timestamp-millis\", \"default\": null }," +
                    //"{\"name\": \"created\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }," +
                    "{\"name\": \"type\", \"type\": \"string\"}," +
                    "{\"name\": \"name\", \"type\": \"string\"}" +
                "]}";

    public AnalysisProducer() throws IOException {
        properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "AnalysisProducer2");
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
            String uuid = UUID.randomUUID().toString();

            System.out.println("...create schema parser");
            Schema.Parser parser = new Schema.Parser();
            System.out.println("...parse schema string");
            Schema schema = parser.parse(schemaString);
            System.out.println("...create GenericRecord");
            GenericRecord genericRecord = new GenericData.Record(schema);

            System.out.println("...put genericRecord values");
            genericRecord.put("type", "event");
            genericRecord.put("name", "add_to_cart");
            genericRecord.put("created", Instant.now().toEpochMilli());


            ProducerRecord<String, GenericRecord> record =
                    new ProducerRecord<>(AnalysisExample.INPUT_TOPIC, uuid, genericRecord);
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
