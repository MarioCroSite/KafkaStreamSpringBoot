package com.mario.stockservice;

import com.mario.events.*;
import com.mario.stockservice.service.KafkaStream;
import com.mario.stockservice.util.MarketUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonSerde;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({OutputCaptureExtension.class})
class TopologyTest extends TestBase {

    private TestInputTopic<String, OrderFullEvent> orderFullEventTopic;
    private TestInputTopic<String, String> orderFullEventStringTopic;
    private TestOutputTopic<String, OrderFullEvent> stockOrdersTopic;
    private KeyValueStore<String, StockReservationEvent> stockStore;
    private TestOutputTopic<String, Error> errorTopic;

    @BeforeEach
    void setup() {
        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var errorSerde = new JsonSerde<>(Error.class);

        orderFullEventTopic = testDriver.createInputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.serializer(), orderFullEventSerde.serializer());
        orderFullEventStringTopic = testDriver.createInputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.serializer(), stringSerde.serializer());
        stockOrdersTopic = testDriver.createOutputTopic(kafkaProperties.getStockOrders(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        stockStore = testDriver.getKeyValueStore(KafkaStream.STORE_NAME);
        errorTopic = testDriver.createOutputTopic("error-topic", stringSerde.deserializer(), errorSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void processSuccessAcceptTopology() {
        String marketId = UUID.randomUUID().toString();
        var stockStore = new StockReservationEvent(MarketUtils.MARKET_AVAILABLE_ITEMS);
        addStockStore(marketId, stockStore);

        var marketItemsBeforeEventIsSend = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, marketItemsBeforeEventIsSend.getItemsAvailable());
        assertEquals(0, marketItemsBeforeEventIsSend.getItemsReserved());

        TestData.orderFullAcceptEvents(marketId).forEach(event -> checkStockStore(event, marketId));
    }

    @Test
    void processSuccessRejectTopology() {
        String marketId = UUID.randomUUID().toString();
        var stockStore = new StockReservationEvent(MarketUtils.MARKET_AVAILABLE_ITEMS);
        addStockStore(marketId, stockStore);

        var marketItemsBeforeEventIsSend = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, marketItemsBeforeEventIsSend.getItemsAvailable());
        assertEquals(0, marketItemsBeforeEventIsSend.getItemsReserved());

        var rejectEvent = TestData.orderFullEventWithProductCount(marketId, 1500);
        orderFullEventTopic.pipeInput(rejectEvent.getId(), rejectEvent);

        var outputEvent = stockOrdersTopic.readKeyValue();
        verifyInputOutputEvent(rejectEvent, outputEvent.value);
        assertEquals(Status.REJECT, outputEvent.value.getStatus());

        var afterEventStore = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, afterEventStore.getItemsAvailable());
        assertEquals(0, afterEventStore.getItemsReserved());
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        orderFullEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");

        assertThat(output.getOut()).contains("Exception caught during Deserialization");

        var rejectEvent = TestData.orderFullEventWithProductCount(UUID.randomUUID().toString(), 1500);
        orderFullEventTopic.pipeInput(rejectEvent.getId(), rejectEvent);

        var outputEvent = stockOrdersTopic.readKeyValue();
        verifyInputOutputEvent(rejectEvent, outputEvent.value);
        assertEquals(Status.REJECT, outputEvent.value.getStatus());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = TestData.orderFullEventWithProductCount(UUID.randomUUID().toString(), 2500);
        orderFullEventTopic.pipeInput(orderEventError.getId(), orderEventError);

        var errorResponseTopic = errorTopic.readKeyValue();
        assertEquals(errorResponseTopic.key, orderEventError.getMarketId());
        assertThat(errorResponseTopic.value.toString()).contains("Product Count is too high");
    }

    private void checkStockStore(OrderFullEvent event, String marketId) {
        var beforeEventStore = getStockStore(marketId);
        orderFullEventTopic.pipeInput(event.getId(), event);
        var afterEventStore = getStockStore(marketId);

        var outputEvent = stockOrdersTopic.readKeyValue();
        verifyInputOutputEvent(event, outputEvent.value);
        assertEquals(Status.ACCEPT, outputEvent.value.getStatus());

        var itemsAvailable = beforeEventStore.getItemsAvailable() - outputEvent.value.getProductCount();
        var itemsReserved = beforeEventStore.getItemsReserved() + outputEvent.value.getProductCount();

        assertEquals(afterEventStore.getItemsAvailable(), itemsAvailable);
        assertEquals(afterEventStore.getItemsReserved(), itemsReserved);

        addStockStore(marketId, new StockReservationEvent(itemsAvailable, itemsReserved));
    }

    private void verifyInputOutputEvent(OrderFullEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProductCount(), output.getProductCount());
        assertEquals(input.getPrice(), output.getPrice());
    }

    private void addStockStore(String key, StockReservationEvent event) {
        stockStore.put(key, event);
    }

    private StockReservationEvent getStockStore(String key) {
        return stockStore.get(key);
    }

}
