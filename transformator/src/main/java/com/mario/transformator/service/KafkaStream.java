package com.mario.transformator.service;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.events.OrderPartialEvent;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.handlers.OrderFullEventTransformer;
import com.mario.transformator.handlers.OrderPartialEventTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
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
                .peek((key, value) -> logger.info("[TRANSFORMER] Key="+ key +", Value="+ value));

        var orderFullEventEvent = incomingOrderEvent.transformValues(OrderFullEventTransformer::new);

        orderFullEventEvent
                .to(kafkaProperties.getOrderFullTopic(), Produced.with(stringSerde, orderFullEventSerde));

        orderFullEventEvent
                .filter((k, v) -> v.getPrice().compareTo(kafkaProperties.getValuableCustomerThreshold()) >= 1)
                .to(kafkaProperties.getValuableCustomer(), Produced.with(stringSerde, orderFullEventSerde));

        //https://www.youtube.com/watch?v=zYGzJYkqUEA&t=250s
        //Split, Merge (productCount)
        var productCountBranch = orderFullEventEvent
                .split(Named.as("branches-"))
                .branch((key, event) -> event.getProductCount() >= 10, Branched.as("full-cart"))
                .branch((key, event) -> event.getProductCount() >= 5, Branched.as("half-full-cart"))
                .defaultBranch(Branched.as("mini-cart"));

        productCountBranch
                .get("branches-full-cart")
                .merge(productCountBranch.get("branches-mini-cart"))
                .mapValues(event -> new OrderPartialEventTransformer().transform(event))
                .to("full-mini-cart", Produced.with(stringSerde, orderPartialEventSerde));

        productCountBranch
                .get("branches-half-full-cart")
                .to("half-full-cart", Produced.with(stringSerde, orderFullEventSerde));


        return incomingOrderEvent;
    }

}
