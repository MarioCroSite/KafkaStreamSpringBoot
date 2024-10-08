package com.mario.transformer.service;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.events.OrderPartialEvent;
import com.mario.transformer.config.KafkaProperties;
import com.mario.transformer.handlers.OrderFullEventTransformer;
import com.mario.transformer.handlers.OrderPartialEventTransformer;
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


@Configuration
public class KafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);

    public static final String STORE_NAME = "ORDERS_STORE";

    @Bean
    public static Topology topology(StreamsBuilder streamsBuilder, KafkaProperties kafkaProperties) {

        var stringSerde = Serdes.String();
        var orderEventSerde = new JsonSerde<>(OrderEvent.class);
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var orderPartialEventSerde = new JsonSerde<>(OrderPartialEvent.class);
        var errorSerde = new JsonSerde<>(Error.class);

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
                .mapValues(ExecutionResult::getError)
                .peek((key, value) -> logger.info("[TRANSFORMER-ERROR] Key="+ key +", Value="+ value))
                .to("error-topic",  Produced.with(stringSerde, errorSerde));

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

        return streamsBuilder.build();
    }

    @Bean
    public KTable<String, OrderEvent> kTable(StreamsBuilder builder, KafkaProperties kafkaProperties) {
        KeyValueBytesStoreSupplier store = Stores.inMemoryKeyValueStore(STORE_NAME);

        var stringSerde = Serdes.String();
        var orderEventSerde = new JsonSerde<>(OrderEvent.class);

        KStream<String, OrderEvent> stream = builder
                .stream(kafkaProperties.getOrderTopic(), Consumed.with(stringSerde, orderEventSerde))
                .peek((key, value) -> logger.info("[ORDER KTable] Key="+ key +", Value="+ value));

        return stream.toTable(Materialized.<String, OrderEvent>as(store)
                .withKeySerde(stringSerde)
                .withValueSerde(orderEventSerde));
    }

}
