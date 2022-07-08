package com.mario.orderservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.orderservice.config.KafkaProperties;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class KafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);

    @Bean
    public static Topology topology(StreamsBuilder streamsBuilder, KafkaProperties kafkaProperties) {
        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);

        var stream = streamsBuilder
                .stream(kafkaProperties.getPaymentOrders(), Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> logger.info("[JOIN BRANCH PAYMENT IN] Key="+ key +", Value="+ value));

        var joiner = stream
                .join(streamsBuilder.stream(kafkaProperties.getStockOrders()),
                        new OrderManager(),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                        StreamJoined.with(stringSerde, orderFullEventSerde, orderFullEventSerde))
                .peek((key, value) -> logger.info("[JOIN BRANCH STOCK IN] Key="+ key +", Value="+ value));

        var joinBranch = joiner
                .split(Named.as("branch-"))
                .branch((key, value) -> value.isSuccess(), Branched.as("success"))
                .defaultBranch(Branched.as("error"));

        joinBranch
                .get("branch-success")
                .mapValues(ExecutionResult::getData)
                .peek((key, value) -> logger.info("[JOIN BRANCH SUCCESS] Key="+ key +", Value="+ value))
                .to(kafkaProperties.getOrderFullTopic());

        joinBranch
                .get("branch-error")
                .mapValues(ExecutionResult::getError)
                .peek((key, value) -> logger.info("[JOIN BRANCH ERROR] Key="+ key +", Value="+ value))
                .to("error-topic");

        return streamsBuilder.build();
    }

//    @Bean
//    public KTable<String, OrderFullEvent> table(StreamsBuilder builder) {
//        KeyValueBytesStoreSupplier store =
//                Stores.persistentKeyValueStore("orders");
//
//        var stringSerde = Serdes.String();
//        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
//
//        KStream<String, OrderFullEvent> stream = builder
//                .stream("orders-full", Consumed.with(stringSerde, orderFullEventSerde))
//                .peek((key, value) -> System.out.println("[ORDER-SERVICE KTable] Key="+ key +", Value="+ value));
//
//        return stream.toTable(Materialized.<String, OrderFullEvent>as(store)
//                .withKeySerde(stringSerde)
//                .withValueSerde(orderFullEventSerde));
//
//    }


}
