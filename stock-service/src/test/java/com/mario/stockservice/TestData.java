package com.mario.stockservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public class TestData {

    public static List<OrderFullEvent> orderFullAcceptEvents(String marketId) {
        String customerId = UUID.randomUUID().toString();

        return List.of(
                OrderFullEventCreator.createOrderFullEvent(
                        UUID.randomUUID().toString(),
                        customerId,
                        marketId,
                        5,
                        BigDecimal.valueOf(30000),
                        List.of(
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 2"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 3"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 4"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 5"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(6000),
                                        "Product 6"
                                )
                        ),
                        Status.NEW
                ),
                OrderFullEventCreator.createOrderFullEvent(
                        UUID.randomUUID().toString(),
                        customerId,
                        marketId,
                        2,
                        BigDecimal.valueOf(10000),
                        List.of(
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 2"
                                ),
                                OrderFullEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 3"
                                )
                        ),
                        Status.NEW
                )
        );
    }

    public static OrderFullEvent orderFullEventWithProductCount(String marketId, Integer productCount) {
        String customerId = UUID.randomUUID().toString();

        return OrderFullEventCreator.createOrderFullEvent(
                UUID.randomUUID().toString(),
                customerId,
                marketId,
                productCount,
                BigDecimal.valueOf(60000),
                List.of(
                        OrderFullEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(30000),
                                "Product 2"
                        ),
                        OrderFullEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(30000),
                                "Product 3"
                        )
                ),
                Status.NEW
        );
    }

}
