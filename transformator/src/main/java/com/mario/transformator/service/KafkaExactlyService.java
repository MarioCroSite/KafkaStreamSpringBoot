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
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaExactlyService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaExactlyService.class);

    KafkaTemplate<String, String> nonTransactional;
    KafkaTemplate<String, String> transactional;
    ObjectMapper objectMapper;
    KafkaProperties kafkaProperties;
    EventRepository eventRepository;
    static int counter = 0;

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
    public void processWithTransactionFirstScenario(OrderFullEvent event) {
        Event event1 = Event.fromOrderFullEvent(event);
        eventRepository.save(event1);
        kafkaSend(transactional, "topic-test-1", event.getId(), event);
        callApi(event.getId());
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        kafkaSend(transactional, "topic-test-2", event.getId(), event);
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void processWithTransactionSecondScenario(OrderFullEvent event) throws InterruptedException, ExecutionException, TimeoutException{
        Event event1 = Event.fromOrderFullEvent(event);
        eventRepository.save(event1);
        counter++;
        if(counter >= 2) {
            kafkaSend(transactional, "topic-test-1", event.getId(), event);
            counter = 0;
        } else {
            try {
                transactional.send("topic-test-1", event.getId(), objectMapper.writeValueAsString(event)).get(1, TimeUnit.MILLISECONDS);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                //var repoAll = eventRepository.findAll();
                throw new KafkaException(e.getCause().getMessage());
            }
        }
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        kafkaSend(transactional, "topic-test-2", event.getId(), event);
    }

    @Transactional
    public void processWithTransactionThirdScenario(OrderFullEvent event) {
        Event event1 = Event.fromOrderFullEvent(event);
        eventRepository.save(event1);
        //kafkaSend(transactional, "topic-test-1", event.getId(), event);
        counter++;
        if(counter >= 2) {
            kafkaSend(transactional, "topic-test-1", event.getId(), event);
            counter = 0;
        } else {
            try {
                 transactional.send("topic-test-1", event.getId(), objectMapper.writeValueAsString(event)).get(1, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                e.printStackTrace();
                //var repoAll = eventRepository.findAll();
                throw new KafkaException(e.getCause().getMessage());
            }
        }

        //callApi(event.getId());
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        kafkaSend(transactional, "topic-test-2", event.getId(), event);
    }

    public void processWithTransactionFourthScenario(OrderFullEvent event) {
        String eventAsString = null;
        try {
            eventAsString = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        String finalEventAsString = eventAsString;

        var isTransactionActive = TransactionSynchronizationManager.isActualTransactionActive();
        logger.info("processWithTransactionFourthScenario {}", isTransactionActive);

        transactional.executeInTransaction(t -> {
            var isTransactionActiveInside = t.inTransaction();
            logger.info("processWithTransactionFourthScenario-INSIDE {}", isTransactionActiveInside);

            Event event1 = Event.fromOrderFullEvent(event);
            eventRepository.save(event1);
            //data is not roll-back from DB

            try {
                t.send("topic-test-1", event.getId(), finalEventAsString); //.get(1, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                var repoAll = eventRepository.findAll();
                e.printStackTrace();
                throw new RuntimeException(e.getCause());
            }

            counter++;
            if(counter >= 2) {
                Event event2 = Event.fromOrderFullEvent(event);
                eventRepository.save(event2);
                counter = 0;
            } else {
                Event event2 = Event.fromOrderFullEventWithEx(event);
                eventRepository.save(event2);
            }
            t.send("topic-test-2", event.getId(), finalEventAsString);
            return true;
        });
    }

    @Transactional(transactionManager = "chainedTransactionManager")
    public void processWithTransactionFifthScenario(OrderFullEvent event) {
        kafkaSend(transactional, "topic-test-1", event.getId(), event);
        counter++;
        if(counter >= 3) {
            Event event2 = Event.fromOrderFullEvent(event);
            eventRepository.save(event2);
            //eventRepository.saveAndFlush(event2);
            counter = 0;
        } else {
            Event event2 = Event.fromOrderFullEventWithEx(event);
            eventRepository.save(event2);
            //eventRepository.saveAndFlush(event2);
        }
        //kafkaSend(transactional, "topic-test-1", event.getId(), event);
        kafkaSend(transactional, "topic-test-2", event.getId(), event);
        Event event2 = Event.fromOrderFullEvent(event);
        eventRepository.save(event2);
        //eventRepository.saveAndFlush(event2);
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
