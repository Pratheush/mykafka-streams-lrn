package com.mytutorial.advancedstreams.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Instant.now;

@Slf4j
public class ProducerUtilWithDelayFunc {
    static KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());

    public static Map<String, Object> producerProps(){

        Map<String,Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propsMap;

    }


    public static RecordMetadata publishMessageSync(String topicName, String key, String message ){

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, key, message);
        return getRecordMetadata(producerRecord);
    }

    /**
     * plusSeconds(delay)
     * Returns a copy of this instant with the specified duration in seconds added.
     * This instance is immutable and unaffected by this method call.
     *
     * toEpochMilli()
     * Converts this instant to the number of milliseconds from the epoch of 1970-01-01T00:00:00Z.
     *
     * producer.send(producerRecord).get();
     * get() :: Waits if necessary for the computation to complete, and then retrieves its result.
     * send() :: Asynchronously send a record to a topic. Equivalent to send(record, null)
     *
     * @param topicName
     * @param key
     * @param message
     * @param delay
     * @return
     */
    public static RecordMetadata publishMessageSyncWithDelay(String topicName, String key, String message , long delay){

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, 0, now().plusSeconds(delay).toEpochMilli(), key, message);
        return getRecordMetadata(producerRecord);
    }

    private static RecordMetadata getRecordMetadata(ProducerRecord<String, String> producerRecord) {
        RecordMetadata recordMetadata=null;
        try {
            log.info("producerRecord : " + producerRecord);
            recordMetadata = producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            log.error("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
        }catch(Exception e){
            log.error("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
        }
        return recordMetadata;
    }

    public static void publishMessageSync(List<ProducerRecord<String, String>> producerRecords) {

        //producerRecords.forEach(producerRecord -> getRecordMetadata(producerRecord));
        producerRecords.forEach(ProducerUtilWithDelayFunc::getRecordMetadata);


    }
}
