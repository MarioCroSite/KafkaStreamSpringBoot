package com.mario.transformator;

import com.mario.transformator.repositories.EventRepository;
import com.mario.transformator.util.WiremockScenario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled
@Testcontainers
public class ExactlyOnceTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceTest.class);

    static List<String> listReadCommitted1 = new ArrayList<>();
    static List<String> listReadCommitted2 = new ArrayList<>();
    static List<String> listReadUnCommitted1 = new ArrayList<>();
    static List<String> listReadUnCommitted2 = new ArrayList<>();

    @Autowired
    EventRepository eventRepository;

    @KafkaListener(
            groupId = "committed",
            topics = {"topic-test-1"},
            containerFactory = "kafkaListenerReadCommittedContainerFactory",
            autoStartup = "true")
    public void receiveCommitted1(String event) {
        logger.info("committed-topic-test-1");
        listReadCommitted1.add(event);
    }

    @KafkaListener(
            groupId = "committed",
            topics = {"topic-test-2"},
            containerFactory = "kafkaListenerReadCommittedContainerFactory",
            autoStartup = "true")
    public void receiveCommitted2(String event) {
        logger.info("committed-topic-test-2");
        listReadCommitted2.add(event);
    }

    @KafkaListener(
            groupId = "uncommitted",
            topics = {"topic-test-1"},
            containerFactory = "kafkaListenerReadUncommittedContainerFactory",
            autoStartup = "true")
    public void receiveUnCommitted1(String event) {
        logger.info("uncommitted-topic-test-1");
        listReadUnCommitted1.add(event);
    }

    @KafkaListener(
            groupId = "uncommitted",
            topics = {"topic-test-2"},
            containerFactory = "kafkaListenerReadUncommittedContainerFactory",
            autoStartup = "true")
    public void receiveUnCommitted2(String event) {
        logger.info("uncommitted-topic-test-2");
        listReadUnCommitted2.add(event);
    }

    @BeforeEach
    void setup() {
        listReadCommitted1.clear();
        listReadCommitted2.clear();
        listReadUnCommitted1.clear();
        listReadUnCommitted2.clear();
        eventRepository.deleteAll();
    }

    @Test
    void transactionalSuccess() {
        var event = TestData.orderEvents().get(1);

        stubWiremock(WiremockScenario.URL_PREFIX + event.getId(), HttpStatus.OK);

        kafkaSend(kafkaProperties.getOrderTopic(), event.getId(), event);

        await().until(() -> listReadCommitted1.size() == 1);
        await().until(() -> listReadCommitted2.size() == 1);
        await().until(() -> listReadUnCommitted1.size() == 1);
        await().until(() -> listReadUnCommitted2.size() == 1);

        assertEquals(eventRepository.findAll().size(), 2);

        verify(exactly(1), getRequestedFor(urlEqualTo(WiremockScenario.URL_PREFIX + event.getId())));
    }

    @Test
    void nonTransactionalSuccess() {
        var event = TestData.orderEvents().get(2);

        stubWiremock(WiremockScenario.URL_PREFIX + event.getId(), HttpStatus.OK);

        kafkaSend(kafkaProperties.getOrderTopic(), event.getId(), event);

        await().until(() -> listReadCommitted1.size() == 1);
        await().until(() -> listReadCommitted2.size() == 1);
        await().until(() -> listReadUnCommitted1.size() == 1);
        await().until(() -> listReadUnCommitted2.size() == 1);

        assertEquals(eventRepository.findAll().size(), 2);

        verify(exactly(1), getRequestedFor(urlEqualTo(WiremockScenario.URL_PREFIX + event.getId())));
    }

    @Test
    void exactlyOnceNonTransactionalConsumer() {
        var event = TestData.orderEvents().get(2);

        stubWiremock(WiremockScenario.URL_PREFIX + event.getId());
        kafkaSend(kafkaProperties.getOrderTopic(), event.getId(), event);

        await().until(() -> listReadCommitted1.size() == 2);
        await().until(() -> listReadCommitted2.size() == 1);
        await().until(() -> listReadUnCommitted1.size() == 2);
        await().until(() -> listReadUnCommitted2.size() == 1);

        assertEquals(eventRepository.findAll().size(), 3);

        verify(exactly(2), getRequestedFor(urlEqualTo(WiremockScenario.URL_PREFIX + event.getId())));
    }

    @Test
    void exactlyOnceTransactionalConsumer() {
        var event = TestData.orderEvents().get(1);

        stubWiremock(WiremockScenario.URL_PREFIX + event.getId());
        kafkaSend(kafkaProperties.getOrderTopic(), event.getId(), event);

        await().until(() -> listReadCommitted1.size() == 1);
        await().until(() -> listReadCommitted2.size() == 1);
        await().until(() -> listReadUnCommitted1.size() == 2);
        await().until(() -> listReadUnCommitted2.size() == 1);

        assertEquals(eventRepository.findAll().size(), 2);

        verify(exactly(2), getRequestedFor(urlEqualTo(WiremockScenario.URL_PREFIX + event.getId())));
    }

}
