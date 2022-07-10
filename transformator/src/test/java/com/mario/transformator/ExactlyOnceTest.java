package com.mario.transformator;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.TimeUnit;

public class ExactlyOnceTest extends TestBase {

    //https://stackoverflow.com/questions/64145670/how-do-i-restart-a-shutdown-embeddedkafkaserver-in-a-spring-unit-test
    //https://github.com/Armando1514/Kafka-Streams-a-nice-introduction/tree/main/exactly-once-semantics
    @Test
    void tester() throws Exception {
        var firstInputEvent = TestData.orderEvents().get(0);
        var secondInputEvent = TestData.orderEvents().get(1);

        SendResult<String, Object> sendResult = kafkaTemplate
                .send(kafkaProperties.getOrderTopic(), objectMapper.writeValueAsString(firstInputEvent))
                .get(10, TimeUnit.SECONDS);
        System.out.println("+++" + sendResult.getRecordMetadata());

        this.embeddedBroker.destroy();
        // restart
        this.embeddedBroker.afterPropertiesSet();

        sendResult = kafkaTemplate
                .send(kafkaProperties.getOrderTopic(), objectMapper.writeValueAsString(secondInputEvent))
                .get(10, TimeUnit.SECONDS);
        System.out.println("+++" + sendResult.getRecordMetadata());
    }

}
