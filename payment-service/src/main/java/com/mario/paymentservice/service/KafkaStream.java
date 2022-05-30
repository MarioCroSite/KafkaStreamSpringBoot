package com.mario.paymentservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.paymentservice.config.KafkaProperties;
import com.mario.paymentservice.domain.ReservationEvent;
import com.mario.paymentservice.handlers.ReservationAggregator;
import com.mario.paymentservice.util.Randomizer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;

@Configuration
public class KafkaStream {

    @Bean
    public KStream<String, OrderFullEvent> kStream(StreamsBuilder streamsBuilder,
                                                   KafkaProperties kafkaProperties) {

        var stringSerde = Serdes.String();
        var orderCalculatedEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var reservationEventSerde = new JsonSerde<>(ReservationEvent.class);

        var incomingOrderCalculatedEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderCalculatedEventSerde))
                .peek((key, value) -> System.out.println("[PAYMENT-SERVICE] Key="+ key +", Value="+ value));

        KeyValueBytesStoreSupplier customerOrderStoreSupplier =
                Stores.persistentKeyValueStore("customer-orders");

        incomingOrderCalculatedEvent
                .selectKey((k, v) -> v.getCustomerId())
                .groupByKey(Grouped.with(stringSerde, orderCalculatedEventSerde))
                .aggregate(
                        () -> new ReservationEvent(Randomizer.generate(BigDecimal.ONE, BigDecimal.valueOf(1000))),
                        new ReservationAggregator(),
                        Materialized.<String, ReservationEvent>as(customerOrderStoreSupplier)
                                .withKeySerde(stringSerde)
                                .withValueSerde(reservationEventSerde))
                .toStream()
                .peek((key, value) -> System.out.println("Key="+ key +", Value="+ value));

        return incomingOrderCalculatedEvent;
    }

}