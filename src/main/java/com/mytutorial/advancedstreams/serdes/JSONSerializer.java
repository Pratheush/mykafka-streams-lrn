package com.mytutorial.advancedstreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mytutorial.advancedstreams.exception.AdvancedStreamException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Creating a Generic Serializer in order to scale up the project if in future there is need
 * because using Generic Serializer we can scale up easily we don't need to create specific type of Serializer and Deserializer
 * like we did create GreetingSerializer and GreetingDeserializer because in future another type came then we have to create
 * again serializer and deserializer which is not recommended way .... we need Generic Serializer to reuse
 *
 * Same goes for Generic Deserializer
 * @param <T>
 */
@Slf4j
public class JSONSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper=new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException JSONSerializer:: serialize :: {}",e.getMessage(),e);
            throw new AdvancedStreamException(e);
        } catch (Exception e){
            log.error("Exception JSONSerializer:: serialize :: {}",e.getMessage(),e);
            throw new AdvancedStreamException(e);
        }
    }
}
