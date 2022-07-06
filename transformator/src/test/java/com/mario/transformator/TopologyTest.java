package com.mario.transformator;

import com.mario.events.*;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.service.KafkaStream;
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
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ExtendWith({OutputCaptureExtension.class})
class TopologyTest {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderEvent> orderEventTopic;
    private TestInputTopic<String, String> orderEventStringTopic;
    private TestOutputTopic<String, OrderFullEvent> orderFullEventTopic;
    private TestOutputTopic<String, OrderFullEvent> valuableCustomerTopic;
    private TestOutputTopic<String, OrderPartialEvent> fullMiniCartTopic;
    private TestOutputTopic<String, OrderFullEvent> halfFullCartTopic;
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
        var orderEventSerde = new JsonSerde<>(OrderEvent.class);
        var orderFullEventSerde = new JsonSerde<>(OrderFullEvent.class);
        var orderPartialEventSerde = new JsonSerde<>(OrderPartialEvent.class);

        orderEventTopic = testDriver.createInputTopic(kafkaProperties.getOrderTopic(), stringSerde.serializer(), orderEventSerde.serializer());
        orderEventStringTopic = testDriver.createInputTopic(kafkaProperties.getOrderTopic(), stringSerde.serializer(), stringSerde.serializer());
        orderFullEventTopic = testDriver.createOutputTopic(kafkaProperties.getOrderFullTopic(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        valuableCustomerTopic = testDriver.createOutputTopic(kafkaProperties.getValuableCustomer(), stringSerde.deserializer(), orderFullEventSerde.deserializer());
        fullMiniCartTopic = testDriver.createOutputTopic("full-mini-cart", stringSerde.deserializer(), orderPartialEventSerde.deserializer());
        halfFullCartTopic = testDriver.createOutputTopic("half-full-cart", stringSerde.deserializer(), orderFullEventSerde.deserializer());
        errorTopic = testDriver.createOutputTopic("error-topic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void processSuccessTopology() {
        var events = orderEvents();
        events.forEach(event -> orderEventTopic.pipeInput(event.getId(), event));

        //first event
        var firstOutputEvent = orderFullEventTopic.readKeyValue();
        var firstInputEvent = findOrderEventById(events, firstOutputEvent.key);
        verifyInputOutputEvent(firstInputEvent, firstOutputEvent.value);

        var fullMiniFirstOutput = fullMiniCartTopic.readKeyValue();
        assertEquals(firstInputEvent.getId(), fullMiniFirstOutput.key);
        verifyInputOutputEvent(firstInputEvent, fullMiniFirstOutput.value);


        //second event
        var secondOutputEvent = orderFullEventTopic.readKeyValue();
        var secondInputEvent = findOrderEventById(events, secondOutputEvent.key);
        verifyInputOutputEvent(secondInputEvent, secondOutputEvent.value);

        var valuableCustomerFirstOutput = valuableCustomerTopic.readKeyValue();
        assertEquals(secondInputEvent.getId(), valuableCustomerFirstOutput.key);
        verifyInputOutputEvent(secondInputEvent, valuableCustomerFirstOutput.value);

        var fullMiniSecondOutput = fullMiniCartTopic.readKeyValue();
        assertEquals(secondInputEvent.getId(), fullMiniSecondOutput.key);
        verifyInputOutputEvent(secondInputEvent, fullMiniSecondOutput.value);


        //third event
        var thirdOutputEvent = orderFullEventTopic.readKeyValue();
        var thirdInputEvent = findOrderEventById(events, thirdOutputEvent.key);
        verifyInputOutputEvent(thirdInputEvent, thirdOutputEvent.value);

        var valuableCustomerSecond = valuableCustomerTopic.readKeyValue();
        assertEquals(thirdInputEvent.getId(), valuableCustomerSecond.key);
        verifyInputOutputEvent(thirdInputEvent, valuableCustomerSecond.value);

        var halfFullFirstOutput = halfFullCartTopic.readKeyValue();
        assertEquals(thirdInputEvent.getId(), halfFullFirstOutput.key);
        verifyInputOutputEvent(thirdInputEvent, halfFullFirstOutput.value);
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        orderEventStringTopic.pipeInput(UUID.randomUUID().toString(), "test");

        assertThat(output.getOut()).contains("Exception caught during Deserialization");

        var events = orderEvents();
        events.forEach(event -> orderEventTopic.pipeInput(event.getId(), event));

        var firstOutputEvent = orderFullEventTopic.readValue();
        var firstInputEvent = findOrderEventById(events, firstOutputEvent.getId());
        verifyInputOutputEvent(firstInputEvent, firstOutputEvent);
    }

    @Test
    void processErrorTopic() {
        var orderEventError = orderEventError();
        orderEventTopic.pipeInput(orderEventError.getId(), orderEventError);

        var errorResponseTopic = errorTopic.readKeyValue();
        assertEquals(errorResponseTopic.key, orderEventError.getId());
        assertThat(errorResponseTopic.value).contains("java.lang.Error: / by zero");
    }

    private void verifyInputOutputEvent(OrderEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProducts().size(), output.getProductCount());
        assertEquals(calculatePrice(input.getProducts()), output.getPrice());
        assertEquals(Status.NEW, output.getStatus());
    }

    private void verifyInputOutputEvent(OrderEvent input, OrderPartialEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getProducts().size(), output.getProductCount());
        assertEquals(calculatePrice(input.getProducts()), output.getPrice());
    }

    private OrderEvent findOrderEventById(List<OrderEvent> events, String id) {
        return events.stream()
                .filter(e -> e.getId().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private BigDecimal calculatePrice(List<Product> products) {
        return products.stream().map(Product::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private List<OrderEvent> orderEvents() {
        String marketId = UUID.randomUUID().toString();
        String customerId = UUID.randomUUID().toString();

        return List.of(
                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 1"
                                )
                        )
                ),

                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(15000),
                                        "Product 1"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 3"
                                )
                        )
                ),

                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 2"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 3"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 4"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 5"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 6"
                                )
                        )
                )
        );
    }

    private OrderEvent orderEventError() {
        return OrderEventCreator.createOrderEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                List.of(
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 2"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 3"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 4"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 5"
                        )
                )
        );
    }

}
