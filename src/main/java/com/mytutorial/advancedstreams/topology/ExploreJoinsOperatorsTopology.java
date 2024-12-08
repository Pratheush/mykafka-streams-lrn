package com.mytutorial.advancedstreams.topology;

import com.mytutorial.advancedstreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.LocalTime;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static final String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static final String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple

    public static final String ALPHABET_TOPIC="alphabets-join";

    public static final String JOINED_STREAM="JOINED-STREAM";

    private ExploreJoinsOperatorsTopology(){}


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // joinKStreamWithKTable(streamsBuilder);
        // joinKStreamWithGlobalKTable(streamsBuilder);
        // joinKTableWithKTable(streamsBuilder);
         joinKStreamWithKStream(streamsBuilder);
        // joinKStreamWithKStreamWithLeftJOIN(streamsBuilder);
        // joinKStreamWithKStreamWithOuterJOIN(streamsBuilder);
        return streamsBuilder.build();
    }

    /**
     * joins will get triggered if there is a matching record for the same key.
     * to achieve a resulting  data model like this, we would need a ValueJoiner.
     * this is also called innerJoin.
     * Join won't happen if the records from topics don't share the same key.
     *
     *
     * so in case of KStream with KTable
     * new events into the KTable doesn't trigger any join
     * but new events into the KSTREAM always trigger join if there is matching key is found in KTable
     * @param streamsBuilder
     */
    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder){

        var alphabetAbbrevationsKStream=streamsBuilder
                        .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));


        var alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                                    .join(alphabetKTable,alphabetValueJoiner);

        // [alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(""));
    }

    /**
     * Here it works same as KStream and KTable Joining
     * However when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data thats part of the KAFKA Topic
     * it's not about a specific instance holding set of keys based on partition that particular task interacts with it's going to have whole representation in those kind of scenarios we need to provide
     * KeyValueMapper that's going to represent what the key is going to be in this case
     * @param streamsBuilder
     */

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder){

        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));


        var alphabetKTable=streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        // GlobalKTable has no toStream() method
        /*alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));*/


        /**
         * when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data thats part of the KAFKA Topic
         * its not about a specific instance holding set of keys based on partition that particular task interacts with its going to have whole representation in those kind of scenarios we need to provide
         * KeyValueMapper thats going to represent what the key is going to be in this case
         */
        // <K> – key type <V> – value type <VR> – mapped value type ; :::: here below this leftKey is from KStream i.e. alphabetAbbrevationsKStream
        // KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> leftKey;
        KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> {
            if (leftKey.equals(rightKey)) return leftKey;
            else return  rightKey;
        };

        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetKTable,keyValueMapper,alphabetValueJoiner);

        // [alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }

    /**
     * In this usecase either side of the data is going to trigger join in our case :::: alphabets_abbreviations table and alphabet table both are KTables so irrespective of whether the data is going to
     * be sent to this alphabets_abbreviations table or alphabet table there will be join triggered.
     * @param streamsBuilder
     */
    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder){

        KTable<String,String> alphabetAbbrevationsKTable=streamsBuilder
                .table(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.<String, String, KeyValueStore<Bytes,byte[]>>as("alphabets-abbreviations-store"));

        alphabetAbbrevationsKTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));


        KTable<String,String> alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        KTable<String,Alphabet> joinedStream=alphabetAbbrevationsKTable
                .join(alphabetKTable,alphabetValueJoiner);

        // [alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder){
        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS+"::"+streamTimeStamp()));


        var alphabetKStream=streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetKStream
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS+"::"+streamTimeStamp()));

        /**
         * Join KStream - KStream ::::
         * The KStream-KStream join is little different compared to the other ones.
         * A KStream is an infinite stream which represents a log of everything that happened
         *
         * JoinWindows:::
         * It is expected that they both share the same key, and also it should be in certain time window( there is a time window defined within the time window those events should be part of the KStream events )
         *
         * so by default any records that gets produced in the KAFKA Topic gets a timestamp attached to it
         *
         *
         * what is the type of join-params
         * StreamJoined<K,V1,V2> this class using which we can provide what the key-value and returned type is going to be
         *
         * if the primary stream begins a window within the 5-second window (here 5-second window is specified as JoinWindows ) which is back and forth which means like if the time is 5:00:00 of the event in primary stream then secondary stream event comes at 4:59:56 (4pm59 minutes and 56 seconds) and 5:00:04 (5 pm 00 minutes and 04 seconds )within that window back and forth
         * if the event comes in the secondary stream then two events or messages from two KStreams will be joined when they both have same matching key
         */

        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        /**
         * Class used to configure the name of the join processor, the repartition topic name, state stores or state store names in Stream-Stream join.
         * Type parameters: * <K> – the key type <V1> – this value type <V2> – other value type
         *
         * StreamJoined is a class that provides utility methods to define the serdes (serializer/deserializer) used when joining two KStreams or KTables.
         * Serdes.String() is a built-in serde for handling strings. It’s being used here for both the keys and values of the streams or tables being joined.
         * StreamJoined.with() is a static factory method that creates a new StreamJoined instance with the specified key, value, and other serdes.
         * In this case, streamJoined is an instance of StreamJoined configured to use String serdes for both keys and values of the joining streams/tables.
         *
         */
        StreamJoined<String, String, String> streamJoined = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetKStream,alphabetValueJoiner,fiveSecondWindow,streamJoined);

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM+"::"+streamTimeStamp()));

    }

    private static void joinKStreamWithKStreamWithLeftJOIN(StreamsBuilder streamsBuilder){
        KStream<String,String> alphabetKStream=streamsBuilder
                                        .stream(ALPHABETS,
                                            Consumed.with(Serdes.String(),Serdes.String()));

        alphabetKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                                                        .stream(ALPHABETS_ABBREVATIONS,
                                                                Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                .withName("alphabets-join")
                .withStoreName("alphabets-join");

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream.leftJoin(alphabetKStream,
                                                alphabetValueJoiner,
                                                fiveSecondWindow,
                                                paramJoins);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));

    }

    /**
     * StreamsBuilder is used to construct a topology of Kafka Streams.
     *
     * alphabetKStream is created by consuming messages from the Kafka topic named ALPHABETS using the stream() method of StreamsBuilder.
     * It's configured to deserialize keys and values as strings.
     * Similarly, alphabetAbbrevationsKStream is created by consuming messages from the Kafka topic named ALPHABETS_ABBREVATIONS
     *
     * ValueJoiner Definition:
     *
     * A ValueJoiner named alphabetValueJoiner is defined. It's a functional interface used to merge the values of the two streams into an instance of Alphabet.
     * The Alphabet::new method is likely a constructor reference used to create an Alphabet object.
     *
     * Configuring Serdes (Serializer/Deserializer):
     * Kafka Streams requires Serdes to serialize and deserialize keys and values when reading from and writing to topics.
     * StreamJoined is used to specify the Serdes for keys and values of both input streams.
     * In the code, Serdes.String() is used for both keys and values, indicating that the keys and values of the streams are expected to be strings.
     *
     * StreamJoined Configuration:
     * StreamJoined is a builder class that configures parameters for joining kafka streams
     * It's configured with key, value, and store serdes (serializer/deserializer) using Serdes.String() for both keys and values.
     * withName() and withStoreName() methods are used to name the joined stream and specify the store name respectively
     * withName(ALPHABET_TOPIC) is used to set the name of the joined stream. This name will be used internally within Kafka Streams.
     * Kafka Streams allows you to store intermediate results of stream processing in state stores. so specify the store-name to store intermediate results of stream processing
     *
     * Join Window Configuration:
     * JoinWindows defines the window settings for the join operation. In this case, a window of 5 seconds is set with no grace period
     *
     * outerJoin()  takes the following parameters
     * alphabetValueJoiner: A function that merges values from both streams into an Alphabet object.
     * fiveSecondWindow: The window configuration for the join operation.
     * paramJoins: Parameters for joining streams, including serdes and store names.
     *
     * KeyValueMapper is a functional interface provided by Kafka Streams. It defines a method apply that takes two parameters - a key and a value - and returns a new key.
     * public interface KeyValueMapper<K, V, R> {
     *     R apply(K key, V value);
     * }
     * The join operation combines each record from the KStream (alphabetAbbrevationsKStream) with the corresponding record from the GlobalKTable (alphabetKTable).
     * However, for this join operation, a mapper is needed to match records from the KStream with records from the GlobalKTable.
     * The KeyValueMapper is used to specify how the key from the KStream (alphabetAbbrevationsKStream) should be matched with the key from the GlobalKTable (alphabetKTable).
     *
     *  KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> leftKey;
     *  This lambda simply takes the key from the left side (KStream) and uses it as the key for the join operation.
     *  means that the records from the KStream are joined with the records from the GlobalKTable based solely on the key from the KStream.
     *  KeyValueMapper is crucial in specifying how keys should be matched between a KStream and a GlobalKTable in the join operation.
     *
     *  KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> {
     *             if (leftKey.equals(rightKey)) return leftKey;
     *             else return  rightKey;
     *         };
     * @param streamsBuilder
     */

    private static void joinKStreamWithKStreamWithOuterJOIN(StreamsBuilder streamsBuilder){

        KStream<String,String> alphabetKStream=streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                .withName(ALPHABET_TOPIC)
                .withStoreName(ALPHABET_TOPIC);

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream.outerJoin(alphabetKStream,
                alphabetValueJoiner,
                fiveSecondWindow,
                paramJoins);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));

    }

    private static String streamTimeStamp(){
        LocalTime now = LocalTime.now();
        return now.toString();
    }


}
