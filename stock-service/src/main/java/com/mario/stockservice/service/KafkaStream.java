package com.mario.stockservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.domain.ReservationEvent;
import com.mario.stockservice.handlers.ReservationAggregator;
import com.mario.stockservice.util.Randomizer;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaStream {

    KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaStream(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Bean
    public KStream<String, OrderFullEvent> kStream(StreamsBuilder streamsBuilder,
                                                   KafkaProperties kafkaProperties) {
        var stringSerde = Serdes.String();
        var orderCalculatedEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var reservationEventSerde = new JsonSerde<>(ReservationEvent.class);

        var incomingOrderCalculatedEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderCalculatedEventSerde))
                .peek((key, value) -> System.out.println("[STOCK-SERVICE] Key="+ key +", Value="+ value));

        KeyValueBytesStoreSupplier stockOrderStoreSupplier =
                Stores.persistentKeyValueStore("stock-orders");

        incomingOrderCalculatedEvent
               .selectKey((k, v) -> v.getMarketId())
               .groupByKey(Grouped.with(stringSerde, orderCalculatedEventSerde))
               .aggregate(
                       () -> new ReservationEvent(Randomizer.generate(1, 1000)),
                       new ReservationAggregator(kafkaTemplate),
                       Materialized.<String, ReservationEvent>as(stockOrderStoreSupplier)
                               .withKeySerde(stringSerde)
                               .withValueSerde(reservationEventSerde)
               )
               .toStream()
               .peek((key, value) -> System.out.println("Key="+ key +", Value="+ value));

        return incomingOrderCalculatedEvent;
    }

}
