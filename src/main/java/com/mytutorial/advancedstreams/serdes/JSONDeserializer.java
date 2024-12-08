package com.mytutorial.advancedstreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.mytutorial.advancedstreams.exception.AdvancedStreamException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JSONDeserializer<T> implements Deserializer<T> {

    private Class<T> destinationClass;
    private final ObjectMapper objectMapper=new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);

    public JSONDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if(data==null) return null; // if data is null i dont want to execute our deserialization logic
        try {
            return objectMapper.readValue(data,destinationClass);
        } catch (IOException e) {
            log.error("IOException JSONDeserializer:: deserialize :: {}",e.getMessage(),e);
            throw new AdvancedStreamException(e);
        }catch (Exception e){
            log.error("Exception JSONDeserializer:: deserialize :: {}",e.getMessage(),e);
            throw new AdvancedStreamException(e);
        }
    }
}
