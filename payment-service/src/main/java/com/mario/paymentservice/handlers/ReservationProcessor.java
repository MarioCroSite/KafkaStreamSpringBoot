package com.mario.paymentservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.PaymentReservationEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.paymentservice.config.KafkaProperties;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.core.KafkaTemplate;

public class ReservationProcessor implements ValueTransformerWithKey<String, OrderFullEvent, ExecutionResult<OrderFullEvent>> {

    private final String storeName;
    private final PaymentReservationEvent initialSeed;
    private KeyValueStore<String, PaymentReservationEvent> stateStore;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    public ReservationProcessor(String storeName,
                                PaymentReservationEvent initialSeed,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                KafkaProperties kafkaProperties) {
        this.storeName = storeName;
        this.initialSeed = initialSeed;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void init(ProcessorContext context) {
        stateStore = context.getStateStore(storeName);
    }

    @Override
    public ExecutionResult<OrderFullEvent> transform(String key, OrderFullEvent orderEvent) {
        var reservation = stateStore.get(key);
        if(reservation == null) {
            reservation = initialSeed;
        }

        try {
            switch (orderEvent.getStatus()) {
                case CONFIRMED:
                    reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
                    break;
                case ROLLBACK:
                    if(orderEvent.getSource() != null && !orderEvent.getSource().equals(Source.PAYMENT)) {
                        reservation.setAmountAvailable(reservation.getAmountAvailable().add(orderEvent.getPrice()));
                        reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
                    }
                    break;
                case NEW:
                    if(orderEvent.getPrice().compareTo(reservation.getAmountAvailable()) <= 1) {
                        reservation.setAmountAvailable(reservation.getAmountAvailable().subtract(orderEvent.getPrice()));
                        reservation.setAmountReserved(reservation.getAmountReserved().add(orderEvent.getPrice()));
                        orderEvent.setStatus(Status.ACCEPT);
                    } else {
                        orderEvent.setStatus(Status.REJECT);
                    }

                    kafkaTemplate.send(kafkaProperties.getPaymentOrders(), orderEvent.getId(), orderEvent);
            }
            stateStore.put(key, reservation);
            return ExecutionResult.success(orderEvent);
        } catch (Exception e) {
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }


    @Override
    public void close() {

    }

}
