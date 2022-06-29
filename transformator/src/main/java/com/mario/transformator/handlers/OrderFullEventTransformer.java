package com.mario.transformator.handlers;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.events.Product;
import com.mario.events.Status;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.math.BigDecimal;

public class OrderFullEventTransformer implements ValueTransformer<OrderEvent, ExecutionResult<OrderFullEvent>> {

    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public ExecutionResult<OrderFullEvent> transform(OrderEvent orderEvent) {
        try {
            return ExecutionResult.success(OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                    .withId(orderEvent.getId())
                    .withCustomerId(orderEvent.getCustomerId())
                    .withMarketId(orderEvent.getMarketId())
                    .withProductCount(orderEvent.getProducts().size())
                    .withPrice(orderEvent.getProducts().stream().map(Product::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add))
                    .withProducts(orderEvent.getProducts())
                    .withStatus(Status.NEW)
                    .build());
        } catch (Exception e) {
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }

    @Override
    public void close() {

    }
}
