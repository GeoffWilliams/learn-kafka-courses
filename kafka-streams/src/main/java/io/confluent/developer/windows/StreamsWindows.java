package io.confluent.developer.windows;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.*;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.*;

public class StreamsWindows {
    public static String formatTimestamp(long timestamp) {
        DateTimeFormatter      formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                .withZone(ZoneId.from(ZoneOffset.UTC));

        Instant instant = java.time.Instant.ofEpochMilli(timestamp);
        return formatter.format(instant);
    }

    public static void main(String[] args) throws IOException {

        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("windowed.input.topic");
        final String outputTopic = streamsProps.getProperty("windowed.output.topic");
        final Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);

        final KStream<String, ElectronicOrder> electronicStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value + "time: " + formatTimestamp(value.getTime())));

        electronicStream.groupByKey()
                // Window the aggregation by the hour and allow for records to be up 5 minutes late
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(5)))
                .aggregate(() -> 0.0,
                           (key, order, total) -> total + order.getPrice(),
                           Materialized.with(Serdes.String(), Serdes.Double()))
                // Don't emit results until the window closes HINT suppression
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                // When windowing Kafka Streams wraps the key in a Windowed class
                // After converting the table to a stream it's a good idea to extract the
                // Underlying key from the Windowed instance HINT: use map
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        TopicLoader.runProducer();
        kafkaStreams.start();
    }
}
