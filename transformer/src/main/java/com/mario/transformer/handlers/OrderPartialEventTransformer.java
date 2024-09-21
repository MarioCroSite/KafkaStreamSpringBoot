package com.mario.transformer.handlers;

import com.mario.events.*;


public class OrderPartialEventTransformer {

    public static OrderPartialEvent transform(OrderFullEvent orderEvent) {
        return OrderPartialEvent.OrderPartialEventBuilder.aOrderPartialEvent()
                .withId(orderEvent.getId())
                .withProductCount(orderEvent.getProductCount())
                .withPrice(orderEvent.getPrice())
                .withProducts(orderEvent.getProducts())
                .build();
    }

}
