package com.mario.orderservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.OrderEvent;
import com.mario.events.Product;
import com.mario.events.Status;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.math.BigDecimal;

public class OrderCalculatedEventTransformer implements ValueTransformer<OrderEvent, OrderFullEvent> {

    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public OrderFullEvent transform(OrderEvent orderEvent) {
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

    @Override
    public void close() {

    }

}
