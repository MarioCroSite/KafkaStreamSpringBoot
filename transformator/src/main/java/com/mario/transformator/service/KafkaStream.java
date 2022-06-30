package com.mario.transformator.service;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.events.OrderPartialEvent;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.handlers.OrderFullEventTransformer;
import com.mario.transformator.handlers.OrderPartialEventTransformer;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;


@Configuration
public class KafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);

    @Bean
    public KStream<String, OrderEvent> kStream(StreamsBuilder streamsBuilder, KafkaProperties kafkaProperties) {

        var stringSerde = Serdes.String();
        var orderEventSerde = new JsonSerde<>(OrderEvent.class);
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var orderPartialEventSerde = new JsonSerde<>(OrderPartialEvent.class);

        var incomingOrderEvent = streamsBuilder
                .stream(kafkaProperties.getOrderTopic(), Consumed.with(stringSerde, orderEventSerde))
                .peek((key, value) -> logger.info("[TRANSFORMER-IN] Key="+ key +", Value="+ value));

        var orderFullEventEvent = incomingOrderEvent.mapValues(new OrderFullEventTransformer());

        var branchOrderFullEvent = orderFullEventEvent
                .split(Named.as("branch-"))
                .branch((key, value) -> value.isSuccess(), Branched.as("success"))
                .defaultBranch(Branched.as("error"));

        var successBranch = branchOrderFullEvent
                .get("branch-success")
                .mapValues(ExecutionResult::getData);

        successBranch
                .peek((key, value) -> logger.info("[TRANSFORMER-OUT] Key="+ key +", Value="+ value))
                .to(kafkaProperties.getOrderFullTopic(), Produced.with(stringSerde, orderFullEventSerde));

        branchOrderFullEvent
                .get("branch-error")
                .mapValues(ExecutionResult::getErrorMessage)
                .peek((key, value) -> logger.info("[TRANSFORMER-ERROR] Key="+ key +", Value="+ value));

        successBranch
                .filter((k, v) -> v.getPrice().compareTo(kafkaProperties.getValuableCustomerThreshold()) >= 1)
                .to(kafkaProperties.getValuableCustomer(), Produced.with(stringSerde, orderFullEventSerde));

        var productCountBranch = successBranch
                .split(Named.as("branches-"))
                .branch((key, event) -> event.getProductCount() >= 10, Branched.as("full-cart"))
                .branch((key, event) -> event.getProductCount() >= 5, Branched.as("half-full-cart"))
                .defaultBranch(Branched.as("mini-cart"));

        productCountBranch
                .get("branches-full-cart")
                .merge(productCountBranch.get("branches-mini-cart"))
                .mapValues(OrderPartialEventTransformer::transform)
                .to("full-mini-cart", Produced.with(stringSerde, orderPartialEventSerde));

        productCountBranch
                .get("branches-half-full-cart")
                .to("half-full-cart", Produced.with(stringSerde, orderFullEventSerde));


        //Topology topology = streamsBuilder.build();
        //System.out.println(topology.describe());
        //System.out.println("TOPOLOGY");
        //https://zz85.github.io/kafka-streams-viz/

        return incomingOrderEvent;
    }

}
