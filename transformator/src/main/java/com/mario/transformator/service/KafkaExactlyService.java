package com.mario.transformator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderFullEvent;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.exception.ApiException;
import com.mario.transformator.models.Event;
import com.mario.transformator.repositories.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Service
public class KafkaExactlyService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaExactlyService.class);

    KafkaTemplate<String, String> nonTransactional;
    KafkaTemplate<String, String> transactional;
    ObjectMapper objectMapper;
    KafkaProperties kafkaProperties;
    EventRepository eventRepository;

    public KafkaExactlyService(@Qualifier("nonTransactional") KafkaTemplate<String, String> nonTransactional,
                               @Qualifier("transactional") KafkaTemplate<String, String> transactional,
                               ObjectMapper objectMapper,
                               KafkaProperties kafkaProperties,
                               EventRepository eventRepository) {
        this.nonTransactional = nonTransactional;
        this.transactional = transactional;
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
        this.eventRepository = eventRepository;
    }

    public void processWithoutTransaction(OrderFullEvent event) {
        Event event1 = Event.fromOrderFullEvent(event);
        eventRepository.save(event1);
        kafkaSend(nonTransactional, "topic-test-1", event.getId(), event);
        callApi(event.getId());
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        kafkaSend(nonTransactional, "topic-test-2", event.getId(), event);
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void processWithTransaction(OrderFullEvent event) {
        Event event1 = Event.fromOrderFullEvent(event);
        eventRepository.save(event1);
        kafkaSend(transactional, "topic-test-1", event.getId(), event);
        callApi(event.getId());
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        kafkaSend(transactional, "topic-test-2", event.getId(), event);
    }

    private void kafkaSend(KafkaTemplate<String, String> template, String topic, String key, Object value) {
        try {
            template.send(topic, key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void callApi(String key) {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response = restTemplate.getForEntity(kafkaProperties.getApiEndpoint() + "/" + key, String.class);
        if(response.getStatusCode() != HttpStatus.OK) {
            logger.error("Error from callApi: " + response.getStatusCodeValue());
            throw new ApiException("callApiError " + response.getStatusCodeValue());
        }
    }

}
