package com.mario.stockservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import com.mario.events.StockReservationEvent;
import com.mario.pojo.ExecutionResult;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.handlers.ReservationProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);
    public static final String STORE_NAME = "MARKET_KAFKA_STORE";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaStream(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public static Topology topology(StreamsBuilder streamsBuilder, KafkaProperties kafkaProperties) {
        streamsBuilder.addStateStore(getStoreBuilder());

        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var errorSerde = new JsonSerde<>(Error.class);
        //var reservationEventSerde = new JsonSerde<>(StockReservationEvent.class);

        var incomingOrderFullEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> logger.info("[STOCK-SERVICE IN] Key="+ key +", Value="+ value));

//        KeyValueBytesStoreSupplier marketOrderStoreSupplier =
//                Stores.persistentKeyValueStore(kafkaProperties.getMarketOrdersStore());

        var aggregateMarketItems = incomingOrderFullEvent
               .selectKey((k, v) -> v.getMarketId())
               .transformValues(() -> new ReservationProcessor(STORE_NAME), STORE_NAME);

//               .groupByKey(Grouped.with(stringSerde, orderFullEventSerde))
//               .aggregate(
//                       () -> new StockReservationEvent(1000),
//                       new ReservationAggregator(kafkaTemplate, kafkaProperties),
//                       Materialized.<String, StockReservationEvent>as(marketOrderStoreSupplier)
//                               .withKeySerde(stringSerde)
//                               .withValueSerde(reservationEventSerde)
//               )
//               .toStream()
//               .peek((key, value) -> logger.info("Key="+ key +", Value="+ value));

        var branchAggregateMarketItems = aggregateMarketItems
                .split(Named.as("branch-"))
                .branch((key, value) -> value.isSuccess(), Branched.as("success"))
                .defaultBranch(Branched.as("error"));

        branchAggregateMarketItems
                .get("branch-success")
                .mapValues(ExecutionResult::getData)
                .peek((key, value) -> logger.info("[STOCK-SERVICE SUCCESS] Key="+ key +", Value="+ value))
                .filter((key, value) -> value.getStatus().equals(Status.ACCEPT) || value.getStatus().equals(Status.REJECT))
                .selectKey((k, v) -> v.getId())
                .peek((key, value) -> logger.info("[STOCK-SERVICE FILTERED NEW] Key="+ key +", Value="+ value))
                .to(kafkaProperties.getStockOrders(), Produced.with(stringSerde, orderFullEventSerde));

        branchAggregateMarketItems
                .get("branch-error")
                .mapValues(ExecutionResult::getError)
                .peek((key, value) -> logger.info("[STOCK-SERVICE ERROR] Key="+ key +", Value="+ value))
                .to("error-topic", Produced.with(stringSerde, errorSerde));

        return streamsBuilder.build();
    }

    private static StoreBuilder<KeyValueStore<String, StockReservationEvent>> getStoreBuilder() {
        return Stores.keyValueStoreBuilder(storeSupplier(), Serdes.String(), new JsonSerde<>(StockReservationEvent.class));
    }

    public static KeyValueBytesStoreSupplier storeSupplier() {
        return Stores.inMemoryKeyValueStore(STORE_NAME);
    }

}
