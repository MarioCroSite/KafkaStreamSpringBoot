package com.mario.paymentservice;

import com.mario.events.OrderFullEvent;
import com.mario.events.Status;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public class TestData {

    public static List<OrderFullEvent> orderFullAcceptEvents(String customerId) {
        String marketId = UUID.randomUUID().toString();

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

    public static OrderFullEvent orderFullEventWithPrice(String customerId, BigDecimal price) {
        String marketId = UUID.randomUUID().toString();

        return OrderFullEventCreator.createOrderFullEvent(
                UUID.randomUUID().toString(),
                customerId,
                marketId,
                2,
                price,
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
