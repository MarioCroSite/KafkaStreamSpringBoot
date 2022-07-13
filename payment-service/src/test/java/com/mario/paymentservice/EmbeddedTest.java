package com.mario.paymentservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.PaymentReservationEvent;
import com.mario.events.Status;
import com.mario.paymentservice.service.KafkaStream;
import com.mario.paymentservice.util.PaymentUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({OutputCaptureExtension.class})
class EmbeddedTest extends TestBase {

    @Test
    void processSuccessAcceptTopology() throws Exception {
        String customerId = UUID.randomUUID().toString();

        TimeUnit.SECONDS.sleep(10);
        var marketItemsBeforeEventIsSend = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, marketItemsBeforeEventIsSend.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, marketItemsBeforeEventIsSend.getAmountReserved());

        TestData.orderFullAcceptEvents(customerId).forEach(event -> checkStockStore(event, customerId));
    }

    @Test
    void processSuccessRejectTopology() throws Exception {
        String customerId = UUID.randomUUID().toString();

        TimeUnit.SECONDS.sleep(10);
        var customerAmountBeforeEventIsSend = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, customerAmountBeforeEventIsSend.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, customerAmountBeforeEventIsSend.getAmountReserved());

        var rejectEvent = TestData.orderFullEventWithPrice(customerId, BigDecimal.valueOf(60000));
        kafkaSend(kafkaProperties.getOrderFullTopic(), rejectEvent.getId(), rejectEvent);
        await().until(() -> paymentOrdersEventTopic.size() == 1);

        var outputEvent = paymentOrdersEventTopic.get(0);
        verifyInputOutputEvent(rejectEvent, outputEvent.value());
        assertEquals(Status.REJECT, outputEvent.value().getStatus());

        var afterEventStore = getStockStore(customerId);
        assertEquals(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE, afterEventStore.getAmountAvailable());
        assertEquals(BigDecimal.ZERO, afterEventStore.getAmountReserved());
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        kafkaSend(kafkaProperties.getOrderFullTopic(), UUID.randomUUID().toString(), "test");
        await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

        var rejectEvent = TestData.orderFullEventWithPrice(UUID.randomUUID().toString(), BigDecimal.valueOf(60000));
        kafkaSend(kafkaProperties.getOrderFullTopic(), rejectEvent.getId(), rejectEvent);
        await().until(() -> paymentOrdersEventTopic.size() == 1);

        var outputEvent = paymentOrdersEventTopic.get(0);
        verifyInputOutputEvent(rejectEvent, outputEvent.value());
        assertEquals(Status.REJECT, outputEvent.value().getStatus());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = TestData.orderFullEventWithPrice(UUID.randomUUID().toString(), BigDecimal.valueOf(100000));
        kafkaSend(kafkaProperties.getOrderFullTopic(), orderEventError.getId(), orderEventError);
        await().until(() -> errorEventTopic.size() == 1);

        var errorResponseTopic = errorEventTopic.get(0);
        assertEquals(errorResponseTopic.key(), orderEventError.getCustomerId());
        assertThat(errorResponseTopic.value().toString()).contains("Price is too high");
    }

    private void checkStockStore(OrderFullEvent event, String customerId) {
        var beforeEventStore = getStockStore(customerId);
        kafkaSend(kafkaProperties.getOrderFullTopic(), event.getId(), event);
        await().until(() -> paymentOrdersEventTopic.size() == 1);
        var afterEventStore = getStockStore(customerId);

        var outputEvent = paymentOrdersEventTopic.get(0);
        verifyInputOutputEvent(event, outputEvent.value());
        assertEquals(Status.ACCEPT, outputEvent.value().getStatus());

        var amountAvailable = beforeEventStore.getAmountAvailable().subtract(outputEvent.value().getPrice());
        var amountReserved = beforeEventStore.getAmountReserved().add(outputEvent.value().getPrice());

        assertEquals(afterEventStore.getAmountAvailable(), amountAvailable);
        assertEquals(afterEventStore.getAmountReserved(), amountReserved);

        paymentOrdersEventTopic.clear();
    }

    private PaymentReservationEvent getStockStore(String key) {
//        ReadOnlyKeyValueStore<String, PaymentReservationEvent> store = streamsFactory
//                .getKafkaStreams()
//                .store(StoreQueryParameters.fromNameAndType(KafkaStream.STORE_NAME, QueryableStoreTypes.keyValueStore()));

        ReadOnlyKeyValueStore<String, PaymentReservationEvent> store = waitUntilStoreIsQueryable(
                KafkaStream.STORE_NAME, QueryableStoreTypes.keyValueStore(), Objects.requireNonNull(streamsFactory.getKafkaStreams()));


        return store.get(key) == null ? new PaymentReservationEvent(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE) : store.get(key);
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType,
                                                  final KafkaStreams streams) {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(storeName, queryableStoreType));
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void verifyInputOutputEvent(OrderFullEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProductCount(), output.getProductCount());
        assertEquals(input.getPrice(), output.getPrice());
    }

}
