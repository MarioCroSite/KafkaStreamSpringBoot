package com.mario.orderservice.service;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.pojo.ExecutionResult;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class OrderManager implements ValueJoiner<OrderFullEvent, OrderFullEvent, ExecutionResult<OrderFullEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(OrderManager.class);

    @Override
    public ExecutionResult<OrderFullEvent> apply(OrderFullEvent payment, OrderFullEvent stock) {
        try {
            OrderFullEvent orderFullEvent = OrderFullEvent.OrderCalculatedEventBuilder.aOrderCalculatedEvent()
                    .withId(payment.getId())
                    .withCustomerId(payment.getCustomerId())
                    .withMarketId(payment.getMarketId())
                    .withProductCount(payment.getProductCount())
                    .withPrice(payment.getPrice())
                    .withProducts(payment.getProducts())
                    .build();

            if(payment.getStatus().equals(Status.ACCEPT) && stock.getStatus().equals(Status.ACCEPT)) {
                orderFullEvent.setStatus(Status.CONFIRMED);
            } else if(payment.getStatus().equals(Status.REJECT) && stock.getStatus().equals(Status.REJECT)) {
                orderFullEvent.setStatus(Status.REJECT);
            } else if(payment.getStatus().equals(Status.REJECT) || stock.getStatus().equals(Status.REJECT)) {
                Source source = payment.getStatus().equals(Status.REJECT) ? Source.PAYMENT : Source.STOCK;
                orderFullEvent.setStatus(Status.ROLLBACK);
                orderFullEvent.setSource(source);
            }

            if(payment.getPrice().compareTo(BigDecimal.valueOf(100000)) >= 0){
                throw new RuntimeException("Price is too high");
            }

            return ExecutionResult.success(orderFullEvent);
        } catch (Exception e) {
            logger.error("Error happen while joining payment and stock {}", e.getMessage());
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }

}
