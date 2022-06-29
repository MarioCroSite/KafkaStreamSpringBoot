package com.mario.stockservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.StockReservationEvent;
import com.mario.pojo.ExecutionResult;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.handlers.ReservationProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
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
    private static final String STORE_NAME = "MARKET_KAFKA_STORE";
    private final StockReservationEvent initialSeed;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaStream(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        initialSeed = new StockReservationEvent(1000);
    }

    @Bean
    public KStream<String, OrderFullEvent> kStream(StreamsBuilder streamsBuilder,
                                                   KafkaProperties kafkaProperties) {
        streamsBuilder.addStateStore(getStoreBuilder());

        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        //var reservationEventSerde = new JsonSerde<>(StockReservationEvent.class);

        var incomingOrderFullEvent = streamsBuilder
                .stream(kafkaProperties.getOrderFullTopic(), Consumed.with(stringSerde, orderFullEventSerde))
                .peek((key, value) -> logger.info("[STOCK-SERVICE IN] Key="+ key +", Value="+ value));

//        KeyValueBytesStoreSupplier marketOrderStoreSupplier =
//                Stores.persistentKeyValueStore(kafkaProperties.getMarketOrdersStore());

        var aggregateMarketItems = incomingOrderFullEvent
               .selectKey((k, v) -> v.getMarketId())
               .transformValues(() -> new ReservationProcessor(STORE_NAME, initialSeed, kafkaTemplate, kafkaProperties), STORE_NAME);

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
                .peek((key, value) -> logger.info("[STOCK-SERVICE SUCCESS] Key="+ key +", Value="+ value));

        branchAggregateMarketItems
                .get("branch-error")
                .peek((key, value) -> logger.info("[STOCK-SERVICE ERROR] Key="+ key +", Value="+ value));

        return incomingOrderFullEvent;
    }

    private StoreBuilder<KeyValueStore<String, StockReservationEvent>> getStoreBuilder() {
        return Stores.keyValueStoreBuilder(storeSupplier(), Serdes.String(), new JsonSerde<>(StockReservationEvent.class));
    }

    public KeyValueBytesStoreSupplier storeSupplier() {
        return Stores.inMemoryKeyValueStore(STORE_NAME);
    }

}
