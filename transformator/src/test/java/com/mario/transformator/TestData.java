package com.mario.transformator;

import com.mario.events.OrderEvent;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public class TestData {

    public static List<OrderEvent> orderEvents() {
        String marketId = UUID.randomUUID().toString();
        String customerId = UUID.randomUUID().toString();

        return List.of(
                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 1"
                                )
                        )
                ),

                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(15000),
                                        "Product 1"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(5000),
                                        "Product 3"
                                )
                        )
                ),

                OrderEventCreator.createOrderEvent(
                        UUID.randomUUID().toString(),
                        marketId,
                        customerId,
                        List.of(
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 2"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 3"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 4"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 5"
                                ),
                                OrderEventCreator.createProduct(
                                        UUID.randomUUID().toString(),
                                        BigDecimal.valueOf(2000),
                                        "Product 6"
                                )
                        )
                )
        );
    }

    public static OrderEvent orderEventError() {
        return OrderEventCreator.createOrderEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                List.of(
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 2"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 3"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 4"
                        ),
                        OrderEventCreator.createProduct(
                                UUID.randomUUID().toString(),
                                BigDecimal.valueOf(2000),
                                "Product 5"
                        )
                )
        );
    }

}
