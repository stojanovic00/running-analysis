package src.util;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.Map;

public class SpeedAggregatorSerializerDeserializer implements Serializer<SpeedAggregator>, Deserializer<SpeedAggregator> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    public SpeedAggregatorSerializerDeserializer(){}
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override
    public byte[] serialize(String topic, SpeedAggregator data) {
        if (data == null ){
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public SpeedAggregator deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        SpeedAggregator data;
        try {
            data = objectMapper.readValue(bytes, SpeedAggregator.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;    }



    @Override
    public void close() {
        Serializer.super.close();
    }
}
