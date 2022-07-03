package com.mario.transformator.handlers;

import com.mario.events.*;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.math.BigDecimal;

public class OrderFullEventTransformer implements ValueMapper<OrderEvent, ExecutionResult<OrderFullEvent>> {

    @Override
    public ExecutionResult<OrderFullEvent> apply(OrderEvent orderEvent) {
        try {
            return ExecutionResult.success(OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                    .withId(orderEvent.getId())
                    .withCustomerId(orderEvent.getCustomerId())
                    .withMarketId(orderEvent.getMarketId())
                    //.withProductCount(orderEvent.getProducts().size())
                    .withProductCount(orderEvent.getProducts().size() == 4 ? (1/0) : orderEvent.getProducts().size()) //for testing error
                    .withPrice(orderEvent.getProducts().stream().map(Product::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add))
                    .withProducts(orderEvent.getProducts())
                    .withStatus(Status.NEW)
                    .build());
        } catch (Exception e) {
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }

}
