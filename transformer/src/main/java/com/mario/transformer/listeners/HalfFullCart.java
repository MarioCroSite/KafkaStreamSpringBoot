package com.mario.transformer.listeners;

import com.mario.events.OrderFullEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class HalfFullCart {

    private static final Logger logger = LoggerFactory.getLogger(FullMiniCart.class);

    @KafkaListener(
            topics = {"${com.mario.kafka.half-full-cart}"},
            groupId = "${com.mario.kafka.consumer-group-id}",
            autoStartup = "true",
            containerFactory = "listenerFactory")
    public void receiveToTopic(OrderFullEvent orderFullEvent) {
        logger.info("[HALF-FULL-CART]");
        logger.info(orderFullEvent.toString());
    }

}
