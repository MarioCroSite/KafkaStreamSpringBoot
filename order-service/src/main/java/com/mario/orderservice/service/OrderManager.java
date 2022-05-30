package com.mario.orderservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import org.springframework.stereotype.Service;

@Service
public class OrderManager {

    public OrderFullEvent confirm(OrderFullEvent payment, OrderFullEvent stock) {
        OrderFullEvent orderFullEvent = OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                .withId(payment.getId())
                .withCustomerId(payment.getCustomerId())
                .withMarketId(payment.getMarketId())
                .withProductCount(payment.getProductCount())
                .withPrice(payment.getPrice())
                .withProducts(payment.getProducts())
                .build();

        if(payment.getStatus().equals(Status.ACCEPT) && stock.getStatus().equals(Status.ACCEPT)) {
            orderFullEvent.setStatus(Status.CONFIRMED);
        } else if(payment.getStatus().equals(Status.REJECT) && stock.getStatus().equals(Status.REJECT)) {
            orderFullEvent.setStatus(Status.REJECT);
        } else if(payment.getStatus().equals(Status.REJECT) || stock.getStatus().equals(Status.REJECT)) {
            Source source = payment.getStatus().equals(Status.REJECT) ? Source.PAYMENT : Source.STOCK;
            orderFullEvent.setStatus(Status.ROLLBACK);
            orderFullEvent.setSource(source);
        }

        return orderFullEvent;
    }

}
