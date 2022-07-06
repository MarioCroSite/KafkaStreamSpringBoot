package com.mario.paymentservice.handlers;

import com.mario.events.*;
import com.mario.paymentservice.config.KafkaProperties;
import com.mario.paymentservice.util.PaymentUtils;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

public class ReservationProcessor implements ValueTransformerWithKey<String, OrderFullEvent, ExecutionResult<OrderFullEvent>> {

    private final String storeName;
    private KeyValueStore<String, PaymentReservationEvent> stateStore;
    //private final KafkaTemplate<String, Object> kafkaTemplate;
    //private final KafkaProperties kafkaProperties;

    public ReservationProcessor(String storeName
                                /*KafkaTemplate<String, Object> kafkaTemplate,
                                KafkaProperties kafkaProperties*/) {
        this.storeName = storeName;
        //this.kafkaTemplate = kafkaTemplate;
        //this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void init(ProcessorContext context) {
        stateStore = context.getStateStore(storeName);
    }

    @Override
    public ExecutionResult<OrderFullEvent> transform(String key, OrderFullEvent orderEvent) {
        var reservation = getStateStore(key);

        try {
            switch (orderEvent.getStatus()) {
                case CONFIRMED:
                    reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
                    break;
                case ROLLBACK:
                    if(orderEvent.getSource() != null && orderEvent.getSource().equals(Source.PAYMENT)) {
                        reservation.setAmountAvailable(reservation.getAmountAvailable().add(orderEvent.getPrice()));
                        reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
                    }
                    break;
                case NEW:
                    if(orderEvent.getPrice().compareTo(reservation.getAmountAvailable()) <= 0) {
                        reservation.setAmountAvailable(reservation.getAmountAvailable().subtract(orderEvent.getPrice()));
                        reservation.setAmountReserved(reservation.getAmountReserved().add(orderEvent.getPrice()));
                        orderEvent.setStatus(Status.ACCEPT);
                    } else {
                        orderEvent.setStatus(Status.REJECT);
                    }

                    //kafkaTemplate.send(kafkaProperties.getPaymentOrders(), orderEvent.getId(), orderEvent);
            }
            if(orderEvent.getPrice().compareTo(BigDecimal.valueOf(100000)) >= 0){
                throw new RuntimeException("Price is too high");
            }
            addStateStore(key, reservation);
            return ExecutionResult.success(orderEvent);
        } catch (Exception e) {
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }

    private PaymentReservationEvent getStateStore(String key) {
        var reservation = stateStore.get(key);
        if(reservation == null) {
            reservation = new PaymentReservationEvent(PaymentUtils.CUSTOMER_AMOUNT_AVAILABLE);
        }
        return reservation;
    }

    private void addStateStore(String key, PaymentReservationEvent value) {
        stateStore.put(key, value);
    }

    @Override
    public void close() {

    }

}
