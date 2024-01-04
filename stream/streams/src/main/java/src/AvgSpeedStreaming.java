package src;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import src.util.SpeedAggregator;
import src.util.SpeedAggregatorSerde;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;

public class AvgSpeedStreaming {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "avg-speed-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        // Additional configurations
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Set the commit interval to 1000 milliseconds (1 second)
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("speed");

        inputStream
                .mapValues(Double::parseDouble)
                .groupByKey()
//                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofMillis(100)).advanceBy(Duration.ofSeconds(1)))
                .aggregate(
                        // The Initializer interface for creating an initial value in aggregations.
                        new Initializer<SpeedAggregator>() {
                            @Override
                            public SpeedAggregator apply() {
                                return new SpeedAggregator();
                            }
                        }, // The Aggregator interface for aggregating values of the given key.
                        new Aggregator<String, Double, SpeedAggregator>() {
                            @Override
                            public SpeedAggregator apply(final String key, final Double value, final SpeedAggregator aggregate) {
                                ++aggregate.count;
                                aggregate.sum += value;
                                return aggregate;
                            }
                        },
                        Materialized.with(Serdes.String(), new SpeedAggregatorSerde())) //How to serialize mid-results
                .toStream()
                .mapValues(new ValueMapper<SpeedAggregator, String>() {
                    @Override
                    public String apply(SpeedAggregator value) {
                        Double average = value.sum / (double) value.count;
                        average = roundDouble(average, 3);
                        System.out.println("sum: " + value.sum + " cnt: " + value.count + " avg: " + average);
                        return average.toString();
                    }
                })
                .to("avg-speed", Produced.with(Serdes.String(), Serdes.String()));

//        inputStream.print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static double roundDouble(double value, int decimalPlaces) {
        if (decimalPlaces < 0) {
            throw new IllegalArgumentException("Decimal places must be non-negative");
        }

        // Create a BigDecimal from the double
        BigDecimal bd = BigDecimal.valueOf(value);

        // Round the BigDecimal to the specified decimal places
        bd = bd.setScale(decimalPlaces, BigDecimal.ROUND_HALF_UP);

        // Get the rounded double value from the BigDecimal
        return bd.doubleValue();
    }

}

