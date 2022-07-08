package com.mario.orderservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.math.BigDecimal;
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
        TestData.orderFullAcceptEvents().forEach(event -> {
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
        var acceptEvent = TestData.orderFullAcceptEvents().get(0);

        kafkaSend(kafkaProperties.getPaymentOrders(), acceptEvent.getId(), acceptEvent);
        TimeUnit.SECONDS.sleep(15);
        kafkaSend(kafkaProperties.getStockOrders(), acceptEvent.getId(), acceptEvent);
        Throwable exception = assertThrows(ConditionTimeoutException.class, () -> await().until(() -> orderFullEventTopic.size() == 1));
        assertThat(exception.getMessage()).contains("was not fulfilled within 10 seconds.");
    }

    @Test
    void processKeyIsNotSameForBothStreamTopology() {
        TestData.orderFullAcceptEvents().forEach(event -> {
            kafkaSend(kafkaProperties.getPaymentOrders(), event.getId(), event);
            kafkaSend(kafkaProperties.getStockOrders(), UUID.randomUUID().toString(), event);
            Throwable exception = assertThrows(ConditionTimeoutException.class, () -> await().until(() -> orderFullEventTopic.size() == 1));
            assertThat(exception.getMessage()).contains("was not fulfilled within 10 seconds.");
            orderFullEventTopic.clear();
        });
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        TestData.orderFullAcceptEvents().forEach(event -> {
            kafkaStringTemplate.send(kafkaProperties.getPaymentOrders(), UUID.randomUUID().toString(), "test");
            await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

            kafkaStringTemplate.send(kafkaProperties.getStockOrders(), UUID.randomUUID().toString(), "test");
            await().until(() -> output.getOut().contains("Exception caught during Deserialization"));
        });
    }

    @Test
    void processErrorTopic() {
        String id = UUID.randomUUID().toString();
        var orderEventError = TestData.orderFullEventWithPrice(id, BigDecimal.valueOf(100000));
        var stockEvent = TestData.orderFullEventWithPrice(id, BigDecimal.valueOf(30000));

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

}
