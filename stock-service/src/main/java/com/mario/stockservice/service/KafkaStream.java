package com.mario.stockservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.StockReservationEvent;
import com.mario.stockservice.config.KafkaProperties;
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
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var reservationEventSerde = new JsonSerde<>(StockReservationEvent.class);

        var incomingOrderCalculatedEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> System.out.println("[STOCK-SERVICE] Key="+ key +", Value="+ value));

        KeyValueBytesStoreSupplier marketOrderStoreSupplier =
                Stores.persistentKeyValueStore(kafkaProperties.getMarketOrdersStore());

        incomingOrderCalculatedEvent
               .selectKey((k, v) -> v.getMarketId())
               .groupByKey(Grouped.with(stringSerde, orderFullEventSerde))
               .aggregate(
                       () -> new StockReservationEvent(Randomizer.generate(1, 50_000)),
                       new ReservationAggregator(kafkaTemplate, kafkaProperties),
                       Materialized.<String, StockReservationEvent>as(marketOrderStoreSupplier)
                               .withKeySerde(stringSerde)
                               .withValueSerde(reservationEventSerde)
               )
               .toStream()
               .peek((key, value) -> System.out.println("Key="+ key +", Value="+ value));

        return incomingOrderCalculatedEvent;
    }

}
