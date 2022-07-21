package com.mario.transformator.listeners;

import com.mario.events.OrderFullEvent;
import com.mario.transformator.service.KafkaExactlyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Service
public class ValuableCustomer {

    private static final Logger logger = LoggerFactory.getLogger(ValuableCustomer.class);

    KafkaExactlyService exactlyService;

    public ValuableCustomer(KafkaExactlyService exactlyService) {
        this.exactlyService = exactlyService;
    }

    @KafkaListener(
            topics = {"${com.mario.kafka.valuable-customer}"},
            groupId = "${com.mario.kafka.consumer-group-id}",
            autoStartup = "true",
            containerFactory = "listenerFactory")
    public void receiveToTopic(OrderFullEvent event) throws ExecutionException, InterruptedException, TimeoutException {
        logger.info("[SENDING E-MAIL TO VALUABLE CUSTOMER]");
        logger.info(event.toString());

        var isTransactionActive = TransactionSynchronizationManager.isActualTransactionActive();
        logger.info("receiveToTopic - transaction {}", isTransactionActive);
        //var status = TransactionAspectSupport.currentTransactionStatus();

        if(event.getProductCount() % 2 == 0) {
            exactlyService.processWithTransactionFirstScenario(event);
            //exactlyService.processWithTransactionSecondScenario(event);
            //exactlyService.processWithTransactionThirdScenario(event);
            //exactlyService.processWithTransactionFourthScenario(event);
            //exactlyService.processWithTransactionFifthScenario(event);
        } else {
            exactlyService.processWithoutTransaction(event);
        }
    }

}
