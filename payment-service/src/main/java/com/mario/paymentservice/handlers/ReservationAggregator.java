//package com.mario.paymentservice.handlers;
//
//import com.mario.events.OrderFullEvent;
//import com.mario.events.PaymentReservationEvent;
//import com.mario.events.Source;
//import com.mario.events.Status;
//import com.mario.paymentservice.config.KafkaProperties;
//import org.apache.kafka.streams.kstream.Aggregator;
//import org.springframework.kafka.core.KafkaTemplate;
//
//public class ReservationAggregator implements Aggregator<String, OrderFullEvent, PaymentReservationEvent> {
//
//    KafkaTemplate<String, Object> kafkaTemplate;
//    KafkaProperties kafkaProperties;
//
//    public ReservationAggregator(KafkaTemplate<String, Object> kafkaTemplate, KafkaProperties kafkaProperties) {
//        this.kafkaTemplate = kafkaTemplate;
//        this.kafkaProperties = kafkaProperties;
//    }
//
//    @Override
//    public PaymentReservationEvent apply(String key, OrderFullEvent orderEvent, PaymentReservationEvent reservation) {
//        switch (orderEvent.getStatus()) {
//            case CONFIRMED:
//                reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
//                break;
//            case ROLLBACK:
//                if(orderEvent.getSource() != null && !orderEvent.getSource().equals(Source.PAYMENT)) {
//                    reservation.setAmountAvailable(reservation.getAmountAvailable().add(orderEvent.getPrice()));
//                    reservation.setAmountReserved(reservation.getAmountReserved().subtract(orderEvent.getPrice()));
//                }
//                break;
//            case NEW:
//                if(orderEvent.getPrice().compareTo(reservation.getAmountAvailable()) <= 1) {
//                    reservation.setAmountAvailable(reservation.getAmountAvailable().subtract(orderEvent.getPrice()));
//                    reservation.setAmountReserved(reservation.getAmountReserved().add(orderEvent.getPrice()));
//                    orderEvent.setStatus(Status.ACCEPT);
//                } else {
//                    orderEvent.setStatus(Status.REJECT);
//                }
//
//                kafkaTemplate.send(kafkaProperties.getPaymentOrders(), orderEvent.getId(), orderEvent);
//        }
//
//        return reservation;
//    }
//
//}
