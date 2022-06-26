package com.mario.transformator.listeners;

import com.mario.events.OrderFullEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ValuableCustomer {

    private static final Logger logger = LoggerFactory.getLogger(ValuableCustomer.class);

    @KafkaListener(
            topics = {"${com.mario.kafka.valuable-customer}"},
            groupId = "${com.mario.kafka.consumer-group-id}",
            autoStartup = "true",
            containerFactory = "listenerFactory")
    public void receiveToTopic(OrderFullEvent event) {
        logger.info("[SENDING E-MAIL TO VALUABLE CUSTOMER]");
        logger.info(event.toString());
    }

}
