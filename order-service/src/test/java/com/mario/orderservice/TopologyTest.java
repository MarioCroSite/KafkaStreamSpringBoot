package com.mario.orderservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import com.mario.orderservice.config.KafkaProperties;
import com.mario.orderservice.service.KafkaStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@ExtendWith({OutputCaptureExtension.class})
class TopologyTest {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderFullEvent> paymentOrdersEventTopic;
    private TestInputTopic<String, String> paymentOrdersEventStringTopic;
    private TestInputTopic<String, OrderFullEvent> stockOrdersEventTopic;
    private TestInputTopic<String, String> stockOrdersEventStringTopic;
    private TestOutputTopic<String, OrderFullEvent> orderFullEventTopic;
    private TestOutputTopic<String, String> errorTopic;

    @Autowired
    KafkaProperties kafkaProperties;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        testDriver = new TopologyTestDriver(KafkaStream.topology(streamsBuilder, kafkaProperties), props);

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
        orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventTopic.pipeInput(event.getId(), event);
            stockOrdersEventTopic.pipeInput(event.getId(), event);

            var outputEvent = orderFullEventTopic.readKeyValue();
            verifyInputOutputEvent(event, outputEvent.value);
            assertEquals(Status.CONFIRMED, outputEvent.value.getStatus());
        });
    }

    @Test
    void processWindowingTopology() {
        orderFullAcceptEvents().forEach(event -> {
            var timestampNow = Instant.now().toEpochMilli();
            var timestampNowPlus15Seconds = Instant.now().plus(15, ChronoUnit.SECONDS).toEpochMilli();

            paymentOrdersEventTopic.pipeInput(event.getId(), event, timestampNow);
            stockOrdersEventTopic.pipeInput(event.getId(), event, timestampNowPlus15Seconds);

            Throwable exception = assertThrows(NoSuchElementException.class, () -> orderFullEventTopic.readKeyValue());
            assertThat(exception.getMessage()).contains("Uninitialized topic: orders-full");
        });
    }

    @Test
    void processKeyisNotSameForBothStreamTopology() {
        orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventTopic.pipeInput(event.getId(), event);
            stockOrdersEventTopic.pipeInput(UUID.randomUUID().toString(), event);

            Throwable exception = assertThrows(NoSuchElementException.class, () -> orderFullEventTopic.readKeyValue());
            assertThat(exception.getMessage()).contains("Uninitialized topic: orders-full");
        });
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        orderFullAcceptEvents().forEach(event -> {
            paymentOrdersEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");
            assertThat(output.getOut()).contains("Exception caught during Deserialization");

            stockOrdersEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");
            assertThat(output.getOut()).contains("Exception caught during Deserialization");
        });
    }

    @Test
    void processErrorTopic() {
        String id = UUID.randomUUID().toString();
        var orderEventError = orderFullEventWithPrice(id, BigDecimal.valueOf(100000));
        var stockEvent = orderFullEventWithPrice(id, BigDecimal.valueOf(30000));

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

    private List<OrderFullEvent> orderFullAcceptEvents() {
        String customerId = UUID.randomUUID().toString();
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
                        Status.ACCEPT
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
                        Status.ACCEPT
                )
        );
    }

    private OrderFullEvent orderFullEventWithPrice(String id, BigDecimal price) {
        String customerId = UUID.randomUUID().toString();
        String marketId = UUID.randomUUID().toString();

        return OrderFullEventCreator.createOrderFullEvent(
                id,
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
