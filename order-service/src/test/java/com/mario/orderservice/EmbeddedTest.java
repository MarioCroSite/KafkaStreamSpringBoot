package com.mario.orderservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({OutputCaptureExtension.class})
class EmbeddedTest extends TestBase {


    @Test
    void processSuccessJoinStreamsTopology() {
        orderFullAcceptEvents().forEach(event -> {
            kafkaSend(kafkaProperties.getPaymentOrders(), event.getId(), event);
            kafkaSend(kafkaProperties.getStockOrders(), event.getId(), event);
            await().until(() -> orderFullEventTopic.size() == 1);

            var outputEvent = orderFullEventTopic.get(0);
            verifyInputOutputEvent(event, outputEvent.value());
            assertEquals(Status.CONFIRMED, outputEvent.value().getStatus());
            orderFullEventTopic.clear();
        });
    }

    @Test
    void processWindowingTopology() throws InterruptedException {
        var acceptEvent = orderFullAcceptEvents().get(0);

        kafkaSend(kafkaProperties.getPaymentOrders(), acceptEvent.getId(), acceptEvent);
        TimeUnit.SECONDS.sleep(15);
        kafkaSend(kafkaProperties.getStockOrders(), acceptEvent.getId(), acceptEvent);
        Throwable exception = assertThrows(ConditionTimeoutException.class, () -> await().until(() -> orderFullEventTopic.size() == 1));
        assertThat(exception.getMessage()).contains("was not fulfilled within 10 seconds.");
    }

    @Test
    void processKeyisNotSameForBothStreamTopology() {
        orderFullAcceptEvents().forEach(event -> {
            kafkaSend(kafkaProperties.getPaymentOrders(), event.getId(), event);
            kafkaSend(kafkaProperties.getStockOrders(), UUID.randomUUID().toString(), event);
            Throwable exception = assertThrows(ConditionTimeoutException.class, () -> await().until(() -> orderFullEventTopic.size() == 1));
            assertThat(exception.getMessage()).contains("was not fulfilled within 10 seconds.");
            orderFullEventTopic.clear();
        });
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        orderFullAcceptEvents().forEach(event -> {
            kafkaStringTemplate.send(kafkaProperties.getPaymentOrders(), UUID.randomUUID().toString(), "test");
            await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

            kafkaStringTemplate.send(kafkaProperties.getStockOrders(), UUID.randomUUID().toString(), "test");
            await().until(() -> output.getOut().contains("Exception caught during Deserialization"));
        });
    }

    @Test
    void processErrorTopic() {
        String id = UUID.randomUUID().toString();
        var orderEventError = orderFullEventWithPrice(id, BigDecimal.valueOf(100000));
        var stockEvent = orderFullEventWithPrice(id, BigDecimal.valueOf(30000));

        kafkaSend(kafkaProperties.getPaymentOrders(), orderEventError.getId(), orderEventError);
        kafkaSend(kafkaProperties.getStockOrders(), stockEvent.getId(), stockEvent);
        await().until(() -> errorEventTopic.size() == 1);

        var errorResponseTopic = errorEventTopic.get(0);
        assertEquals(errorResponseTopic.key(), orderEventError.getId());
        assertThat(errorResponseTopic.value().toString()).contains("Price is too high");
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
