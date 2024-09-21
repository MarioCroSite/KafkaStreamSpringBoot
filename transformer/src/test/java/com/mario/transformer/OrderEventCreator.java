package com.mario.transformer;

import com.mario.events.OrderEvent;
import com.mario.events.Product;

import java.math.BigDecimal;
import java.util.List;

public class OrderEventCreator {

    public static OrderEvent createOrderEvent(String id, String marketId, String customerId, List<Product> products) {
        return OrderEvent.OrderEventBuilder.aOrderEvent()
                .withId(id)
                .withMarketId(marketId)
                .withCustomerId(customerId)
                .withProducts(products)
                .build();
    }


    public static Product createProduct(String id, BigDecimal price, String name) {
        return Product.ProductBuilder.aProduct()
                .withId(id)
                .withPrice(price)
                .withName(name)
                .build();
    }

}
