package com.mytutorial.advancedstreams.serdes;

import com.mytutorial.advancedstreams.domain.Alphabet;
import com.mytutorial.advancedstreams.domain.AlphabetWordAggregate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * this SerdesFactory class is created to use it with Consumed.with() and Produced.with()
 * there in that in key section its String so Serdes.String() is used but in value section it is Greeting in JSON format
 * so we created the SerdesFactory which has greeting() method which returns GreetingSerdes which is type of Serde<Greeting>
 *
 *     just like Serdes.String() method is used in Consumed.with() and Produced.with()
 *
 *     in Serdes.class
 *     public static Serde<String> String() {
 *         return new StringSerde();
 *     }
 *
 *     public static final class StringSerde extends WrapperSerde<String> {
 *         public StringSerde() {
 *             super(new StringSerializer(), new StringDeserializer());
 *         }
 *     }
 *
 *     public static class WrapperSerde<T> implements Serde<T> {} this WrapperSerde implements Serde of type that we specify as Target
 *
 */
public class SerdesFactory {


    // in future if we want a different type we basically create another factory function then we change the type over here
    // like here we used Greeting we can use any other type accordingly
    public static Serde<AlphabetWordAggregate> alphabetWordAggregate() {

        JSONSerializer<AlphabetWordAggregate> jsonSerializer = new JSONSerializer<>();

        JSONDeserializer<AlphabetWordAggregate> jsonDeSerializer = new JSONDeserializer<>(AlphabetWordAggregate.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }


    public static Serde<Alphabet> alphabet() {

        JSONSerializer<Alphabet> jsonSerializer = new JSONSerializer<>();

        JSONDeserializer<Alphabet> jsonDeSerializer = new JSONDeserializer<>(Alphabet.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
