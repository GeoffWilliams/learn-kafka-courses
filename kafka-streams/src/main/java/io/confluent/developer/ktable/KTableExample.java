package io.confluent.developer.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KTableExample {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-application");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("ktable.input.topic");
        final String outputTopic = streamsProps.getProperty("ktable.output.topic");

        final String orderNumberStart = "orderNumber-";

        // Crate a table with the StreamBuilder from above and use the table method
        // along with the inputTopic create a Materialized instance and name the store
        // and provide a Serdes for the key and the value  HINT: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as
        // then use two methods to specify the key and value serde
        KTable<String, String> firstKTable = builder.table(inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));
        
        firstKTable.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .toStream()
                // Then uncomment the following two lines to view results on the console and write to a topic
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        TopicLoader.runProducer();
        kafkaStreams.start();
    }
}
