package com.mario.paymentservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.PaymentReservationEvent;
import com.mario.paymentservice.config.KafkaProperties;
import com.mario.paymentservice.handlers.ReservationAggregator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;

@Configuration
public class KafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);

    KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaStream(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public KStream<String, OrderFullEvent> kStream(StreamsBuilder streamsBuilder,
                                                   KafkaProperties kafkaProperties) {

        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var reservationEventSerde = new JsonSerde<>(PaymentReservationEvent.class);

        var incomingOrderCalculatedEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> logger.info("[PAYMENT-SERVICE] Key="+ key +", Value="+ value));

        KeyValueBytesStoreSupplier customerOrderStoreSupplier =
                Stores.persistentKeyValueStore(kafkaProperties.getCustomerOrdersStore());

        incomingOrderCalculatedEvent
                .selectKey((k, v) -> v.getCustomerId())
                .groupByKey(Grouped.with(stringSerde, orderFullEventSerde))
                .aggregate(
                        () -> new PaymentReservationEvent(BigDecimal.valueOf(50000)),
                        new ReservationAggregator(kafkaTemplate, kafkaProperties),
                        Materialized.<String, PaymentReservationEvent>as(customerOrderStoreSupplier)
                                .withKeySerde(stringSerde)
                                .withValueSerde(reservationEventSerde))
                .toStream()
                .peek((key, value) -> logger.info("Key="+ key +", Value="+ value));

        return incomingOrderCalculatedEvent;
    }

}
