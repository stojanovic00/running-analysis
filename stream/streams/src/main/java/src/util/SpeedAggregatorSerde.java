package src.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class SpeedAggregatorSerde implements Serde<SpeedAggregator> {
    @Override
    public Serializer<SpeedAggregator> serializer() {
        return new SpeedAggregatorSerializerDeserializer();
    }

    @Override
    public Deserializer<SpeedAggregator> deserializer() {
        return new SpeedAggregatorSerializerDeserializer();
    }
}
