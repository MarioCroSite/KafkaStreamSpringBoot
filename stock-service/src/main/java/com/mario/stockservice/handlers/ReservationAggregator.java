package com.mario.stockservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.events.StockReservationEvent;
import com.mario.stockservice.config.KafkaProperties;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.kafka.core.KafkaTemplate;

public class ReservationAggregator implements Aggregator<String, OrderFullEvent, StockReservationEvent> {

    KafkaTemplate<String, Object> kafkaTemplate;
    KafkaProperties kafkaProperties;

    public ReservationAggregator(KafkaTemplate<String, Object> kafkaTemplate, KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public StockReservationEvent apply(String key, OrderFullEvent orderEvent, StockReservationEvent reservation) {
        switch (orderEvent.getStatus()) {
            case CONFIRMED:
                reservation.setItemsReserved(reservation.getItemsReserved() - orderEvent.getProductCount());
                break;
            case ROLLBACK:
                if(orderEvent.getSource() != null && !orderEvent.getSource().equals(Source.STOCK)) {
                    reservation.setItemsAvailable(reservation.getItemsAvailable() + orderEvent.getProductCount());
                    reservation.setItemsReserved(reservation.getItemsReserved() - orderEvent.getProductCount());
                }
                break;
            case NEW:
                if(orderEvent.getProductCount() <= reservation.getItemsAvailable()) {
                    reservation.setItemsAvailable(reservation.getItemsAvailable() - orderEvent.getProductCount());
                    reservation.setItemsReserved(reservation.getItemsReserved() + orderEvent.getProductCount());
                    orderEvent.setStatus(Status.ACCEPT);
                } else {
                    orderEvent.setStatus(Status.REJECT);
                }

                kafkaTemplate.send(kafkaProperties.getStockOrders(), orderEvent.getId(), orderEvent);
        }

        return reservation;
    }

}
