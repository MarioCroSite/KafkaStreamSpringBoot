package com.mario.generator.services;

import com.mario.events.OrderEvent;
import com.mario.generator.config.KafkaProperties;
import com.mario.generator.util.Randomizer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class GeneratorService {

    KafkaTemplate<String, Object> kafkaTemplate;
    KafkaProperties kafkaProperties;

    public GeneratorService(KafkaTemplate<String, Object> kafkaTemplate, KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @Scheduled(fixedRate = 60000) //every minute
    public void orderEventGenerator() {
        OrderEvent event = OrderEvent.OrderEventBuilder.aOrderEvent()
                .withId(UUID.randomUUID().toString())
                .withCustomerId(Randomizer.getCustomerId())
                .withProducts(Randomizer.getProducts())
                .withMarketId(Randomizer.getMarketId())
                .build();

        kafkaTemplate.send(kafkaProperties.getOrderTopic(), event.getId(), event);
    }

}
