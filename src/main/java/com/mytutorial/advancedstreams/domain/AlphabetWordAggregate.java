package com.mytutorial.advancedstreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount) {


    public AlphabetWordAggregate() {
        this("", new HashSet<>(), 0);
    }


    // everytime the function is invoked we are going to update the runningCount
    public AlphabetWordAggregate updateNewEvents(String key, String newValue){
        log.info("Before the update : {} , valueList :: {}", this, valueList );
        log.info("New Record : key : {} , value : {} : ", key, newValue );
        var newRunningCount = this.runningCount +1;
        valueList.add(newValue);
        var aggregated = new AlphabetWordAggregate(key, valueList, newRunningCount);
        log.info("aggregated : {}" , aggregated);
        return aggregated;
    }


    public static void main(String[] args) {


        var al =new AlphabetWordAggregate();

    }

}
