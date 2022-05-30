package com.mario.stockservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.stockservice.domain.ReservationEvent;
import org.apache.kafka.streams.kstream.Aggregator;

public class ReservationAggregator implements Aggregator<String, OrderFullEvent, ReservationEvent> {

    @Override
    public ReservationEvent apply(String s, OrderFullEvent orderEvent, ReservationEvent reservation) {
        switch (orderEvent.getStatus()) {
            case CONFIRMED:
                reservation.setItemsReserved(reservation.getItemsReserved() - orderEvent.getProductCount());
            case ROLLBACK:
                if(!orderEvent.getSource().equals(Source.STOCK)) {
                    reservation.setItemsAvailable(reservation.getItemsAvailable() + orderEvent.getProductCount());
                    reservation.setItemsReserved(reservation.getItemsReserved() + orderEvent.getProductCount());
                }
            case NEW:
                if(orderEvent.getProductCount() <= reservation.getItemsAvailable()) {
                    reservation.setItemsAvailable(reservation.getItemsAvailable() - orderEvent.getProductCount());
                    reservation.setItemsReserved(reservation.getItemsReserved() + orderEvent.getProductCount());
                    orderEvent.setStatus(Status.ACCEPT);
                } else {
                    orderEvent.setStatus(Status.REJECT);
                }
        }

        return reservation;
    }

}
