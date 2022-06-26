package com.mario.transformator.handlers;

import com.mario.events.*;


public class OrderPartialEventTransformer {

    public OrderPartialEvent transform(OrderFullEvent orderEvent) {
        return OrderPartialEvent.OrderPartialEventBuilder.aOrderPartialEvent()
                .withId(orderEvent.getId())
                .withProductCount(orderEvent.getProductCount())
                .withPrice(orderEvent.getPrice())
                .withProducts(orderEvent.getProducts())
                .build();
    }

}
