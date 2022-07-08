package com.mario.orderservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({OutputCaptureExtension.class})
class TopologyTest extends TestBase {

    private TestInputTopic<String, OrderFullEvent> paymentOrdersEventTopic;
    private TestInputTopic<String, String> paymentOrdersEventStringTopic;
    private TestInputTopic<String, OrderFullEvent> stockOrdersEventTopic;
    private TestInputTopic<String, String> stockOrdersEventStringTopic;
    private TestOutputTopic<String, OrderFullEvent> orderFullEventTopic;
    private TestOutputTopic<String, String> errorTopic;

    @BeforeEach
    void setup() {
        var stringSerde = Serdes.String();
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);

        paymentOrdersEventTopic = testDriver.createInputTopic(kafkaProperties.getPaymentOrders(), stringSerde.serializer(), orderFullEventSerde.serializer());
        paymentOrdersEventStringTopic = testDriver.createInputTopic(kafkaProperties.getPaymentOrders(), stringSerde.serializer(), stringSerde.serializer());
        stockOrdersEventTopic = testDriver.createInputTopic(kafkaProperties.getStockOrders(), stringSerde.serializer(), orderFullEventSerde.serializer());
        stockOrdersEventStringTopic = testDriver.createInputTopic(kafkaProperties.getStockOrders(), stringSerde.serializer(), stringSerde.serializer());
        orderFullEventTopic = testDriver.createOutputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        errorTopic = testDriver.createOutputTopic("error-topic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void processSuccessJoinStreamsTopology() {
        TestData.orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventTopic.pipeInput(event.getId(), event);
            stockOrdersEventTopic.pipeInput(event.getId(), event);

            var outputEvent = orderFullEventTopic.readKeyValue();
            verifyInputOutputEvent(event, outputEvent.value);
            assertEquals(Status.CONFIRMED, outputEvent.value.getStatus());
        });
    }

    @Test
    void processWindowingTopology() {
        TestData.orderFullAcceptEvents().forEach(event -> {
            var timestampNow = Instant.now().toEpochMilli();
            var timestampNowPlus15Seconds = Instant.now().plus(15, ChronoUnit.SECONDS).toEpochMilli();

            paymentOrdersEventTopic.pipeInput(event.getId(), event, timestampNow);
            stockOrdersEventTopic.pipeInput(event.getId(), event, timestampNowPlus15Seconds);

            Throwable exception = assertThrows(NoSuchElementException.class, () -> orderFullEventTopic.readKeyValue());
            assertThat(exception.getMessage()).contains("Uninitialized topic: orders-full");
        });
    }

    @Test
    void processKeyIsNotSameForBothStreamTopology() {
        TestData.orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventTopic.pipeInput(event.getId(), event);
            stockOrdersEventTopic.pipeInput(UUID.randomUUID().toString(), event);

            Throwable exception = assertThrows(NoSuchElementException.class, () -> orderFullEventTopic.readKeyValue());
            assertThat(exception.getMessage()).contains("Uninitialized topic: orders-full");
        });
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        TestData.orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");
            assertThat(output.getOut()).contains("Exception caught during Deserialization");

            stockOrdersEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");
            assertThat(output.getOut()).contains("Exception caught during Deserialization");
        });
    }

    @Test
    void processErrorTopic() {
        String id = UUID.randomUUID().toString();
        var orderEventError = TestData.orderFullEventWithPrice(id, BigDecimal.valueOf(100000));
        var stockEvent = TestData.orderFullEventWithPrice(id, BigDecimal.valueOf(30000));

        paymentOrdersEventTopic.pipeInput(orderEventError.getId(), orderEventError);
        stockOrdersEventTopic.pipeInput(stockEvent.getId(), stockEvent);

        var errorResponseTopic = errorTopic.readKeyValue();
        assertEquals(errorResponseTopic.key, orderEventError.getId());
        assertThat(errorResponseTopic.value).contains("Price is too high");
    }

    private void verifyInputOutputEvent(OrderFullEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProductCount(), output.getProductCount());
        assertEquals(input.getPrice(), output.getPrice());
    }

}
