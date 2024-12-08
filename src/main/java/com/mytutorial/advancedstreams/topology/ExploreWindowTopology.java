package com.mytutorial.advancedstreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> wordKStream=streamsBuilder
                .stream(WINDOW_WORDS,Consumed.with(Serdes.String(),Serdes.String()));

        // tumblingWindow(wordKStream);
        // hoppingWindow(wordKStream);
        slidingWindow(wordKStream);

        return streamsBuilder.build();
    }


    private static void tumblingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize=Duration.ofSeconds(5);

        TimeWindows timeWindows=TimeWindows.ofSizeWithNoGrace(fiveSecondWindowSize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String> and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek(((key, value) -> {
                    log.info("tumblingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }


    private static void hoppingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize = Duration.ofSeconds(5);
        Duration advanceBySize = Duration.ofSeconds(3);

        TimeWindows timeWindows=TimeWindows
                .ofSizeWithNoGrace(fiveSecondWindowSize)
                .advanceBy(advanceBySize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String> and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek(((key, value) -> {
                    log.info("hoppingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }

    private static void slidingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize = Duration.ofSeconds(5);

        var slidingWindow = SlidingWindows.ofTimeDifferenceWithNoGrace(fiveSecondWindowSize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(slidingWindow)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String> and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek(((key, value) -> {
                    log.info("slidingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }



    // each window has startTime and endTime
    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime(); // startTime type is Instant
        var endTime = key.window().endTime();   // any time windows are created it is going to be in gmt time not the localTime
        log.info("startTime : {}, endTime : {}, Count : {}", startTime, endTime, value); // here printing the instant startTime and endTime  :: windowed key startTime and endTime are in gmt format

        // converting the instant value into local timestamp using zone since i am in IST i.e entry("IST", "Asia/Kolkata"),. we can get the code by clicking into SHORT_IDS
        // getting the localtime from the startTime and endTime
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }


}