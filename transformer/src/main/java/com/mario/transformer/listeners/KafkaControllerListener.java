package com.mario.transformer.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaControllerListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaControllerListener.class);

    @KafkaListener(
            groupId = "committed",
            topics = {"tester-1"},
            containerFactory = "readCommittedContainerFactory",
            autoStartup = "true")
    public void receiveCommitted1(String event) {
        logger.info("committed-tester-1");
    }

    @KafkaListener(
            groupId = "uncommitted",
            topics = {"tester-1"},
            containerFactory = "readUncommittedContainerFactory",
            autoStartup = "true")
    public void receiveUnCommitted1(String event) {
        logger.info("uncommitted-tester-1");
    }

}
