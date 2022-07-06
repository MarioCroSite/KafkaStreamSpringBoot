package com.mario.paymentservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.PaymentReservationEvent;
import com.mario.events.Status;
import com.mario.paymentservice.config.KafkaProperties;
import com.mario.paymentservice.service.KafkaStream;
import com.mario.paymentservice.util.PaymentUtils;
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
    private TestOutputTopic<String, OrderFullEvent> paymentOrdersTopic;
    private KeyValueStore<String, PaymentReservationEvent> stockStore;
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
        paymentOrdersTopic = testDriver.createOutputTopic(kafkaProperties.getPaymentOrders(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        stockStore = testDriver.getKeyValueStore(KafkaStream.STORE_NAME);
        errorTopic = testDriver.createOutputTopic("error-topic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void processSuccessAcceptTopology() {
        String customerId = UUID.randomUUID().toString();
        var paymentStore = new PaymentReservationEvent(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE);
        addStockStore(customerId, paymentStore);

        var customerAmountBeforeEventIsSend = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, customerAmountBeforeEventIsSend.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, customerAmountBeforeEventIsSend.getAmountReserved());

        orderFullAcceptEvents(customerId).forEach(event -> checkStockStore(event, customerId));
    }

    @Test
    void processSuccessRejectTopology() {
        String customerId = UUID.randomUUID().toString();
        var paymentStore = new PaymentReservationEvent(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE);
        addStockStore(customerId, paymentStore);

        var customerAmountBeforeEventIsSend = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, customerAmountBeforeEventIsSend.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, customerAmountBeforeEventIsSend.getAmountReserved());

        var rejectEvent = orderFullEventWithPrice(customerId, BigDecimal.valueOf(60000));
        orderFullEventTopic.pipeInput(rejectEvent.getId(), rejectEvent);

        var outputEvent = paymentOrdersTopic.readKeyValue();
        verifyInputOutputEvent(rejectEvent, outputEvent.value);
        assertEquals(Status.REJECT, outputEvent.value.getStatus());

        var afterEventStore = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, afterEventStore.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, afterEventStore.getAmountReserved());
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        orderFullEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");

        assertThat(output.getOut()).contains("Exception caught during Deserialization");

        var rejectEvent = orderFullEventWithPrice(UUID.randomUUID().toString(), BigDecimal.valueOf(60000));
        orderFullEventTopic.pipeInput(rejectEvent.getId(), rejectEvent);

        var outputEvent = paymentOrdersTopic.readKeyValue();
        verifyInputOutputEvent(rejectEvent, outputEvent.value);
        assertEquals(Status.REJECT, outputEvent.value.getStatus());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = orderFullEventWithPrice(UUID.randomUUID().toString(), BigDecimal.valueOf(100000));
        orderFullEventTopic.pipeInput(orderEventError.getId(), orderEventError);

        var errorResponseTopic = errorTopic.readKeyValue();
        assertEquals(errorResponseTopic.key, orderEventError.getCustomerId());
        assertThat(errorResponseTopic.value).contains("Price is too high");
    }

    private void checkStockStore(OrderFullEvent event, String customerId) {
        var beforeEventStore = getStockStore(customerId);
        orderFullEventTopic.pipeInput(event.getId(), event);
        var afterEventStore = getStockStore(customerId);

        var outputEvent = paymentOrdersTopic.readKeyValue();
        verifyInputOutputEvent(event, outputEvent.value);
        assertEquals(Status.ACCEPT, outputEvent.value.getStatus());

        var amountAvailable = beforeEventStore.getAmountAvailable().subtract(outputEvent.value.getPrice());
        var amountReserved = beforeEventStore.getAmountReserved().add(outputEvent.value.getPrice());

        assertEquals(afterEventStore.getAmountAvailable(), amountAvailable);
        assertEquals(afterEventStore.getAmountReserved(), amountReserved);

        addStockStore(customerId, new PaymentReservationEvent(amountAvailable, amountReserved));
    }

    private void verifyInputOutputEvent(OrderFullEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProductCount(), output.getProductCount());
        assertEquals(input.getPrice(), output.getPrice());
    }

    private void addStockStore(String key, PaymentReservationEvent event) {
        stockStore.put(key, event);
    }

    private PaymentReservationEvent getStockStore(String key) {
        return stockStore.get(key);
    }

    private List<OrderFullEvent> orderFullAcceptEvents(String customerId) {
        String marketId = UUID.randomUUID().toString();

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

    private OrderFullEvent orderFullEventWithPrice(String customerId, BigDecimal price) {
        String marketId = UUID.randomUUID().toString();

        return OrderFullEventCreator.createOrderFullEvent(
                UUID.randomUUID().toString(),
                customerId,
                marketId,
                2,
                price,
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
