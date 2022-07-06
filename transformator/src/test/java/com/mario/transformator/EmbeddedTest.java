package com.mario.transformator;

import com.mario.events.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({OutputCaptureExtension.class})
class EmbeddedTest extends TestBase {


    @Test
    void processSuccessTopology() throws Exception {
        var firstInputEvent = orderEvents().get(0);
        var secondInputEvent = orderEvents().get(1);
        var thirdInputEvent = orderEvents().get(2);


        //first event
        kafkaSend(kafkaProperties.getOrderTopic(), firstInputEvent.getId(), firstInputEvent);
        await().until(() -> orderFullEventTopic.size() == 1);
        await().until(() -> fullMiniCartEventTopic.size() == 1);

        var firstOutputEvent = orderFullEventTopic.get(0);
        verifyInputOutputEvent(firstInputEvent, firstOutputEvent.value());

        var fullMiniFirstOutput = fullMiniCartEventTopic.get(0);
        assertEquals(firstInputEvent.getId(), fullMiniFirstOutput.key());
        verifyInputOutputEvent(firstInputEvent, fullMiniFirstOutput.value());


        //second event
        kafkaSend(kafkaProperties.getOrderTopic(), secondInputEvent.getId(), secondInputEvent);
        await().until(() -> orderFullEventTopic.size() == 2);
        await().until(() -> valuableCustomerEventTopic.size() == 1);
        await().until(() -> fullMiniCartEventTopic.size() == 2);

        var secondOutputEvent = orderFullEventTopic.get(1);
        verifyInputOutputEvent(secondInputEvent, secondOutputEvent.value());

        var valuableCustomerFirstOutput = valuableCustomerEventTopic.get(0);
        assertEquals(secondInputEvent.getId(), valuableCustomerFirstOutput.key());
        verifyInputOutputEvent(secondInputEvent, valuableCustomerFirstOutput.value());

        var fullMiniSecondOutput = fullMiniCartEventTopic.get(1);
        assertEquals(secondInputEvent.getId(), fullMiniSecondOutput.key());
        verifyInputOutputEvent(secondInputEvent, fullMiniSecondOutput.value());


        //third event
        kafkaSend(kafkaProperties.getOrderTopic(), thirdInputEvent.getId(), thirdInputEvent);
        await().until(() -> orderFullEventTopic.size() == 3);
        await().until(() -> valuableCustomerEventTopic.size() == 2);
        await().until(() -> halfFullCartEventTopic.size() == 1);

        var thirdOutputEvent = orderFullEventTopic.get(2);
        verifyInputOutputEvent(thirdInputEvent, thirdOutputEvent.value());

        var valuableCustomerSecond = valuableCustomerEventTopic.get(1);
        assertEquals(thirdInputEvent.getId(), valuableCustomerSecond.key());
        verifyInputOutputEvent(thirdInputEvent, valuableCustomerSecond.value());

        var halfFullFirstOutput = halfFullCartEventTopic.get(0);
        assertEquals(thirdInputEvent.getId(), halfFullFirstOutput.key());
        verifyInputOutputEvent(thirdInputEvent, halfFullFirstOutput.value());
    }

    @Test
    void processDeserializationError(CapturedOutput output) throws Exception {
        kafkaSend(kafkaProperties.getOrderTopic(), UUID.randomUUID().toString(), "test");
        await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

        var firstInputEvent = orderEvents().get(0);
        kafkaSend(kafkaProperties.getOrderTopic(), firstInputEvent.getId(), firstInputEvent);
        await().until(() -> orderFullEventTopic.size() == 1);
        verifyInputOutputEvent(firstInputEvent, orderFullEventTopic.get(0).value());
    }

    @Test
    void processErrorTopic() throws Exception{
        var orderEventError = orderEventError();
        kafkaSend(kafkaProperties.getOrderTopic(), orderEventError.getId(), orderEventError);
        await().until(() -> errorEventTopic.size() == 1);

        var errorResponseTopic = errorEventTopic.get(0);
        assertEquals(errorResponseTopic.key(), orderEventError.getId());
        assertThat(errorResponseTopic.value().toString()).contains("java.lang.Error: / by zero");
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
