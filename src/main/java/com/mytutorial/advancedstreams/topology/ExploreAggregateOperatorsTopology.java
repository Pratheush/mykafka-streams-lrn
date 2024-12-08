package com.mytutorial.advancedstreams.topology;

import com.mytutorial.advancedstreams.domain.AlphabetWordAggregate;
import com.mytutorial.advancedstreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static final String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder
                .stream(AGGREGATE,
                        Consumed.with(Serdes.String(),Serdes.String()));

        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        // groupByKey() takes the key automatically from the actual record and then apply grouping on it
        // so if the key is we would like to change from the actual Kafka Record that's when we use operator groupBy()
        // groupBy() accepts the KeyValueMapper basically its a key selector so the advantage is that we can provide what the new key is going to be
        // we can alter the Key Also . For some usecases there might be no key in Kafka Records Streaming from the Kafka Topic
        // in this case we have the key as A and B in some cases there might not be any key in those kind of scenarios we can use
        // groupBy() operator to change a key or even if there is an existing key and we would like to change the key to different value then use groupBy() operator
        // anytime we perform groupBy() its recommended to provide the type also >> here  Grouped.with(Serdes.String(), Serdes.String())
        KGroupedStream<String,String> groupedString = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())) // here key is same as in Kafka Records from Internal Kafka Topic
                //        .groupBy((key, value) -> value ,                // here key is now changed value has become key and below type of key-value is provided which is recommended.
                //                Grouped.with(Serdes.String(),Serdes.String()))
                ;

         exploreCount(groupedString);
        exploreReduce(groupedString);
        // exploreAggregate(groupedString);

        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {

        // when we do something like this what Kafka Stream does is that it creates an internal Kafka Topic and maintains the data in the Kafka Topic
        // but there is no way to query the data if we are using this approach i.e. without using Materialized
        // if we are using Materialized then instead of creating Internal Kafka Topic of its own its going to create state store and maintain the data in the internal Kafka Topic

        KTable<String,Long> countByAlphabet = groupedStream
                //.count(Named.as("count-per-alphabet"))
                .count(Named.as("COUNT-PER-ALPHABET"),
                Materialized.as("COUNT-PER-ALPHABET-STORE"))
                ;

        countByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("words-count-per-alphabet"));

    }

    // behind the scenes Kafka Stream did create  internal Kafka Topic and stored the complete state over there
    // for FaultTolerance and retention
    private static void exploreReduce(KGroupedStream<String, String> groupedStream){
        KTable<String,String> reducedKtable=groupedStream
                                        .reduce((previousValue, currentValue) -> {
                                           log.info("PreviousValue :: {} , CurrentValue :: {}",previousValue,currentValue);
                                           return  previousValue.toUpperCase() + "-" + currentValue.toUpperCase();
                                        }
                                        , Materialized.<String,String,KeyValueStore<Bytes,byte[]>>as("REDUCED-WORDS-STORE")
                                                        .withKeySerde(Serdes.String())
                                                        .withValueSerde(Serdes.String())
                                        );
        reducedKtable.toStream()
                .print(Printed.<String, String>toSysOut().withLabel("REDUCED-WORDS"));
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedStream){
        // Initializer is java bean which is going to represent JSON that has three properties and we are instatiating  new instance of it. Type here is initializer.
        // creating new empty instance
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer= AlphabetWordAggregate::new;

        // this is the actual code which is going to perform aggregation
        // as we are getting new value we are calling updateNewEvents()
        //  aggregator :: where we are updating the running_count and update the array-value with the new value as the new values come in .
        Aggregator<String,String,AlphabetWordAggregate> alphabetWordAggregateAggregator=
                (key, value, alphabetWordAggregate) -> alphabetWordAggregate.updateNewEvents(key, value);

        // since we are changing the type from one to another because we are changing from type String to AlphabetWordAggregate
        // the better option is to provide Materialized . Materialized is one of the option of providing our own State Store instead of Kafka taking care to create that State Store for us.
        // Materialized.<String,AlphabetWordAggregate >as("aggregated-store") first is key second is the value  third is providing what kind of state store we are going to use.
        // materializing the aggregated value to a stateStore
// as this is needed anytime the application is restarted and the app needs to reconstruct whole stream / state
// the reason why Materialized is used in this usecase  not for the other one is that because the type here is going to be little different because the value is going to be new object or a new type.
// its not going to be String anymore .that is why we are using materialized . so in aggregate operator we have to use Materialized view but in count and reduce operator if we don't use Materialized then there won't be any problems.
        // advantage of using Materialized views when saving the state of the Aggregated Operation
        KTable<String,AlphabetWordAggregate>alphabetWordAggregateKTable=groupedStream
                                            .aggregate(alphabetWordAggregateInitializer,
                                                    alphabetWordAggregateAggregator,
                                                    Materialized.<String,AlphabetWordAggregate, KeyValueStore<Bytes,byte[]>>as("AGGREGATED-WORDS-STORE")
                                                            .withKeySerde(Serdes.String()) // specifying key serde
                                                            .withValueSerde(SerdesFactory.alphabetWordAggregate()) // specifying value serde
                                                    );
        alphabetWordAggregateKTable.toStream()
                .print(Printed.<String,AlphabetWordAggregate>toSysOut().withLabel("AGGREGATED-WORDS"));
    }

}
