package com.mario.paymentservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Product;
import com.mario.events.Status;

import java.math.BigDecimal;
import java.util.List;

public class OrderFullEventCreator {

    public static OrderFullEvent createOrderFullEvent(String id, String customerId, String marketId, Integer productCount,
                                                      BigDecimal price, List<Product> product, Status status) {
        return OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                .withId(id)
                .withCustomerId(customerId)
                .withMarketId(marketId)
                .withProductCount(productCount)
                .withPrice(price)
                .withProducts(product)
                .withStatus(status)
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
