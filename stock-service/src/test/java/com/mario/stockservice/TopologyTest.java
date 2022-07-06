package com.mario.stockservice;

import com.mario.events.*;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.service.KafkaStream;
import com.mario.stockservice.util.MarketUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ExtendWith({OutputCaptureExtension.class})
class TopologyTest {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderFullEvent> orderFullEventTopic;
    private TestInputTopic<String, String> orderFullEventStringTopic;
    private TestOutputTopic<String, OrderFullEvent> stockOrdersTopic;
    private KeyValueStore<String, StockReservationEvent> stockStore;
    private TestOutputTopic<String, String> errorTopic;

    @Autowired
    KafkaProperties kafkaProperties;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        testDriver = new TopologyTestDriver(KafkaStream.topology(streamsBuilder, kafkaProperties), props);

        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);

        orderFullEventTopic = testDriver.createInputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.serializer(), orderFullEventSerde.serializer());
        orderFullEventStringTopic = testDriver.createInputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.serializer(), stringSerde.serializer());
        stockOrdersTopic = testDriver.createOutputTopic(kafkaProperties.getStockOrders(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        stockStore = testDriver.getKeyValueStore(KafkaStream.STORE_NAME);
        errorTopic = testDriver.createOutputTopic("error-topic", stringSerde.deserializer(), stringSerde.deserializer());
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

        orderFullAcceptEvents(marketId).forEach(event -> checkStockStore(event, marketId));
    }

    @Test
    void processSuccessRejectTopology() {
        String marketId = UUID.randomUUID().toString();
        var stockStore = new StockReservationEvent(MarketUtils.MARKET_AVAILABLE_ITEMS);
        addStockStore(marketId, stockStore);

        var marketItemsBeforeEventIsSend = getStockStore(marketId);
        assertEquals(MarketUtils.MARKET_AVAILABLE_ITEMS, marketItemsBeforeEventIsSend.getItemsAvailable());
        assertEquals(0, marketItemsBeforeEventIsSend.getItemsReserved());

        var rejectEvent = orderFullEventWithProductCount(marketId, 1500);
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

        var rejectEvent = orderFullEventWithProductCount(UUID.randomUUID().toString(), 1500);
        orderFullEventTopic.pipeInput(rejectEvent.getId(), rejectEvent);

        var outputEvent = stockOrdersTopic.readKeyValue();
        verifyInputOutputEvent(rejectEvent, outputEvent.value);
        assertEquals(Status.REJECT, outputEvent.value.getStatus());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = orderFullEventWithProductCount(UUID.randomUUID().toString(), 2500);
        orderFullEventTopic.pipeInput(orderEventError.getId(), orderEventError);

        var errorResponseTopic = errorTopic.readKeyValue();
        assertEquals(errorResponseTopic.key, orderEventError.getMarketId());
        assertThat(errorResponseTopic.value).contains("Product Count is too high");
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
