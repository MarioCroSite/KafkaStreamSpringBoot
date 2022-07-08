package com.mario.stockservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import com.mario.events.StockReservationEvent;
import com.mario.stockservice.service.KafkaStream;
import com.mario.stockservice.util.MarketUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({OutputCaptureExtension.class})
class EmbeddedTest extends TestBase {

    @Test
    void processSuccessAcceptTopology() {
        String marketId = UUID.randomUUID().toString();

        var marketItemsBeforeEventIsSend = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, marketItemsBeforeEventIsSend.getItemsAvailable());
        assertEquals(0, marketItemsBeforeEventIsSend.getItemsReserved());

        orderFullAcceptEvents(marketId).forEach(event -> checkStockStore(event, marketId));
    }

    @Test
    void processSuccessRejectTopology() {
        String marketId = UUID.randomUUID().toString();

        var marketItemsBeforeEventIsSend = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, marketItemsBeforeEventIsSend.getItemsAvailable());
        assertEquals(0, marketItemsBeforeEventIsSend.getItemsReserved());

        var rejectEvent = orderFullEventWithProductCount(marketId, 1500);
        kafkaSend(kafkaProperties.getOrderFullTopic(), rejectEvent.getId(), rejectEvent);
        await().until(() -> stockOrdersEventTopic.size() == 1);

        var outputEvent = stockOrdersEventTopic.get(0);
        verifyInputOutputEvent(rejectEvent, outputEvent.value());
        assertEquals(Status.REJECT, outputEvent.value().getStatus());

        var afterEventStore = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, afterEventStore.getItemsAvailable());
        assertEquals(0, afterEventStore.getItemsReserved());
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        kafkaSend(kafkaProperties.getOrderFullTopic(), UUID.randomUUID().toString(), "test");
        await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

        var rejectEvent = orderFullEventWithProductCount(UUID.randomUUID().toString(), 1500);
        kafkaSend(kafkaProperties.getOrderFullTopic(), rejectEvent.getId(), rejectEvent);
        await().until(() -> stockOrdersEventTopic.size() == 1);

        var outputEvent = stockOrdersEventTopic.get(0);
        verifyInputOutputEvent(rejectEvent, outputEvent.value());
        assertEquals(Status.REJECT, outputEvent.value().getStatus());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = orderFullEventWithProductCount(UUID.randomUUID().toString(), 2500);
        kafkaSend(kafkaProperties.getOrderFullTopic(), orderEventError.getId(), orderEventError);
        await().until(() -> errorEventTopic.size() == 1);

        var errorResponseTopic = errorEventTopic.get(0);
        assertEquals(errorResponseTopic.key(), orderEventError.getMarketId());
        assertThat(errorResponseTopic.value().toString()).contains("Product Count is too high");
    }

    private void checkStockStore(OrderFullEvent event, String marketId) {
        var beforeEventStore = getStockStore(marketId);
        kafkaSend(kafkaProperties.getOrderFullTopic(), event.getId(), event);
        await().until(() -> stockOrdersEventTopic.size() == 1);
        var afterEventStore = getStockStore(marketId);

        var outputEvent = stockOrdersEventTopic.get(0);
        verifyInputOutputEvent(event, outputEvent.value());
        assertEquals(Status.ACCEPT, outputEvent.value().getStatus());

        var itemsAvailable = beforeEventStore.getItemsAvailable() - outputEvent.value().getProductCount();
        var itemsReserved = beforeEventStore.getItemsReserved() + outputEvent.value().getProductCount();

        assertEquals(afterEventStore.getItemsAvailable(), itemsAvailable);
        assertEquals(afterEventStore.getItemsReserved(), itemsReserved);

        stockOrdersEventTopic.clear();
    }

    private StockReservationEvent getStockStore(String key) {
        ReadOnlyKeyValueStore<String, StockReservationEvent> store = streamsFactory
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(KafkaStream.STORE_NAME, QueryableStoreTypes.keyValueStore()));

        return store.get(key) == null ? new StockReservationEvent(MarketUtils.MARKET_AVAILABLE_ITEMS) : store.get(key);
    }

    private void addStockStore(String key, StockReservationEvent event) {
        KeyValueStore<String, StockReservationEvent> store = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(KafkaStream.STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(StockReservationEvent.class))
                .build();

        store.put(key, event);
    }

    private void verifyInputOutputEvent(OrderFullEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProductCount(), output.getProductCount());
        assertEquals(input.getPrice(), output.getPrice());
    }

    private List<OrderFullEvent> orderFullAcceptEvents(String marketId) {
        String customerId = UUID.randomUUID().toString();

        return List.of(
                OrderFullEventCreator.createOrderFullEvent(
                        UUID.randomUUID().toString(),
                        customerId,
                        marketId,
                        5,
                        BigDecimal.valueOf(30000),
                        List.of(
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 2"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 3"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 4"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 5"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 6"
                                )
                        ),
                        Status.NEW
                ),
                OrderFullEventCreator.createOrderFullEvent(
                        UUID.randomUUID().toString(),
                        customerId,
                        marketId,
                        2,
                        BigDecimal.valueOf(10000),
                        List.of(
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 2"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 3"
                                )
                        ),
                        Status.NEW
                )
        );
    }

    private OrderFullEvent orderFullEventWithProductCount(String marketId, Integer productCount) {
        String customerId = UUID.randomUUID().toString();

        return OrderFullEventCreator.createOrderFullEvent(
                UUID.randomUUID().toString(),
                customerId,
                marketId,
                productCount,
                BigDecimal.valueOf(60000),
                List.of(
                        OrderFullEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(30000),
                                "Product 2"
                        ),
                        OrderFullEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(30000),
                                "Product 3"
                        )
                ),
                Status.NEW
        );
    }

}
