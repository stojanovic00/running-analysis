package src;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class HeartRateStreaming {

    public static void main(String[] args) {
        // Configure Kafka Streams properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "heart-rate-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        // Additional configurations
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> statsStream = builder.stream("run_stats");

        // Extract the value from the input and put it into the "heart_rate" topic
        statsStream
                .filter((key, value) -> key != null && value != null)
                .mapValues(HeartRateStreaming::extractHeartRate)
                .to("heart_rate");

        // Print the output to the console for debugging purposes
        statsStream.print(Printed.toSysOut());

        // Build the KafkaStreams object and start the application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Add a shutdown hook to close the KafkaStreams object gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String extractHeartRate(String input) {
        return input.split(",")[5];
    }
}
