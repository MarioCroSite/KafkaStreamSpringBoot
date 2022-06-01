package com.mario.orderservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.orderservice.config.KafkaProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class KafkaStream {


    @Autowired
    OrderManager orderManager;

    @Bean
    public KStream<String, OrderFullEvent> kStream(StreamsBuilder streamsBuilder,
                                                   KafkaProperties kafkaProperties) {
        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);

        var stream = streamsBuilder
                .stream("payment-orders", Consumed.with(stringSerde, orderFullEventSerde));

        stream.join(streamsBuilder.stream("stock-orders"),
                orderManager::confirm,
                JoinWindows.of(Duration.ofSeconds(10)),
                StreamJoined.with(stringSerde, orderFullEventSerde, orderFullEventSerde))
                .peek((key, value) -> System.out.println("[ORDER-SERVICE] Key="+ key +", Value="+ value))
                .to("orders-full");

        return stream;
    }

    @Bean
    public KTable<String, OrderFullEvent> table(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier store =
                Stores.persistentKeyValueStore("orders");

        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);

        KStream<String, OrderFullEvent> stream = builder
                .stream("orders-full", Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> System.out.println("[ORDER-SERVICE KTable] Key="+ key +", Value="+ value));

        return stream.toTable(Materialized.<String, OrderFullEvent>as(store)
                .withKeySerde(stringSerde)
                .withValueSerde(orderFullEventSerde));

    }


}
