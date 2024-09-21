package com.mario.transformer.controllers.mappers;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.events.Product;
import com.mario.events.Status;

import java.math.BigDecimal;

public class OrderFullEventMapper {

    public static OrderFullEvent fromOrderEventToOrderFullEvent(OrderEvent orderEvent) {
        return OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                .withId(orderEvent.getId())
                .withCustomerId(orderEvent.getCustomerId())
                .withMarketId(orderEvent.getMarketId())
                .withProductCount(orderEvent.getProducts().size())
                .withPrice(orderEvent.getProducts().stream().map(Product::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add))
                .withProducts(orderEvent.getProducts())
                .withStatus(Status.NEW)
                .build();
    }

}
