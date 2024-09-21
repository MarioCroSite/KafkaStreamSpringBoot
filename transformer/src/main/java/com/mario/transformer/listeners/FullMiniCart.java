package com.mario.transformer.listeners;

import com.mario.events.OrderPartialEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FullMiniCart {

    private static final Logger logger = LoggerFactory.getLogger(FullMiniCart.class);

    @KafkaListener(
            topics = {"${com.mario.kafka.full-mini-cart}"},
            groupId = "${com.mario.kafka.consumer-group-id}",
            autoStartup = "true",
            containerFactory = "listenerFactory")
    public void receiveToTopic(OrderPartialEvent orderPartialEvent) {
        logger.info("[FULL-MINI-CART]");
        logger.info(orderPartialEvent.toString());
    }

}
