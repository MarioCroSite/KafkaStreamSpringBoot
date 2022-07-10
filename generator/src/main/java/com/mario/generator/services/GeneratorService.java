package com.mario.generator.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    ObjectMapper objectMapper;

    public GeneratorService(KafkaTemplate<String, Object> kafkaTemplate, KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.objectMapper = objectMapper;
    }

//    @Scheduled(fixedRate = 60000) //every minute
//    public void orderEventGenerator() throws JsonProcessingException {
//        var event = generateEvent();
//        kafkaTemplate.send(kafkaProperties.getOrderTopic(), event.getId(), objectMapper.writeValueAsString(event));
//    }

    @Scheduled(fixedRate = 5000) //every 5 sec
    public void orderGenerator() {
        kafkaTemplate.executeInTransaction(template -> {
            try {
                var event1 = generateEvent();
                template.send(kafkaProperties.getOrderTopic(), event1.getId(), objectMapper.writeValueAsString(event1));

                template.send(kafkaProperties.getOrderTopic(), UUID.randomUUID().toString(), "test");

                var event2 = generateEvent();
                template.send(kafkaProperties.getOrderTopic(), event2.getId(), objectMapper.writeValueAsString(event2));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        });
    }

    private OrderEvent generateEvent() {
        return OrderEvent.OrderEventBuilder.aOrderEvent()
                .withId(UUID.randomUUID().toString())
                .withCustomerId(Randomizer.getCustomerId())
                .withProducts(Randomizer.getProducts())
                .withMarketId(Randomizer.getMarketId())
                .build();
    }

}
