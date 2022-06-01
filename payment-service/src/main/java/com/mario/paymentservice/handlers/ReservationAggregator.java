package com.mario.paymentservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.paymentservice.domain.ReservationEvent;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.kafka.core.KafkaTemplate;

public class ReservationAggregator implements Aggregator<String, OrderFullEvent, ReservationEvent> {

    KafkaTemplate<String, Object> kafkaTemplate;

    public ReservationAggregator(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public ReservationEvent apply(String s, OrderFullEvent orderEvent, ReservationEvent reservation) {
        switch (orderEvent.getStatus()) {
            case CONFIRMED:
                reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
            case ROLLBACK:
                if(orderEvent.getSource() != null && !orderEvent.getSource().equals(Source.PAYMENT)) {
                    reservation.setAmountAvailable(reservation.getAmountAvailable().add(orderEvent.getPrice()));
                    reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
                }
            case NEW:
                if(orderEvent.getPrice().compareTo(reservation.getAmountAvailable()) <= 1) {
                    reservation.setAmountAvailable(reservation.getAmountAvailable().subtract(orderEvent.getPrice()));
                    reservation.setAmountReserved(reservation.getAmountReserved().add(orderEvent.getPrice()));
                    orderEvent.setStatus(Status.ACCEPT);
                } else {
                    orderEvent.setStatus(Status.REJECT);
                }

                kafkaTemplate.send("payment-orders", orderEvent.getId(), orderEvent);
        }

        return reservation;
    }

}
