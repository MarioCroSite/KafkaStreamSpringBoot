package com.mario.transformator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderEvent;
import com.mario.transformator.controllers.mappers.OrderFullEventMapper;
import com.mario.transformator.models.Event;
import com.mario.transformator.repositories.EventRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

@Service
public class KafkaControllerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaControllerService.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, String> kafkaTemplateTx;
    private EventRepository eventRepository;
    private ObjectMapper objectMapper;

    public KafkaControllerService(@Qualifier("nonTransactional") KafkaTemplate<String, String> kafkaTemplate,
                                  @Qualifier("transactional") KafkaTemplate<String, String> kafkaTemplateTx,
                                  EventRepository eventRepository,
                                  ObjectMapper objectMapper) {
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplateTx = kafkaTemplateTx;
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void sendToKafkaAndDBScenario1(OrderEvent event) {
        eventRepository.save(toEvent(event));
        eventRepository.save(toEventWithEx(event));
        kafkaSendTx("tester-1", event.getId(), event);
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void sendToKafkaAndDBScenario2(OrderEvent event) {
        eventRepository.save(toEvent(event));
        kafkaSendTx("tester-1", event.getId(), event);
        secondsSleep(5);
        eventRepository.save(toEventWithEx(event));
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void sendToKafkaAndDBScenario3(OrderEvent event) {
        kafkaSendTx("tester-1", event.getId(), event);
        secondsSleep(5);
        eventRepository.save(toEvent(event));
        eventRepository.save(toEventWithEx(event));
    }

    @Bean
    public NewTopic tester1() {
        return TopicBuilder.name("tester-1")
                .partitions(3)
                .replicas(2)
                .build();
    }

    private Event toEvent(OrderEvent event) {
        return Event.fromOrderFullEvent(OrderFullEventMapper.fromOrderEventToOrderFullEvent(event));
    }

    private Event toEventWithEx(OrderEvent event) {
        return Event.fromOrderFullEventWithEx(OrderFullEventMapper.fromOrderEventToOrderFullEvent(event));
    }

    private void kafkaSend(String topic, String key, Object value) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void kafkaSendTx(String topic, String key, Object value) {
        try {
            kafkaTemplateTx.send(topic, key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void secondsSleep(long second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
