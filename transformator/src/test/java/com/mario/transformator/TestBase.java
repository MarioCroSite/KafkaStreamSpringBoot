package com.mario.transformator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderFullEvent;
import com.mario.events.OrderPartialEvent;
import com.mario.transformator.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"${com.mario.kafka.order-topic}"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
public class TestBase {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    ObjectMapper objectMapper;

    static List<ConsumerRecord<String, OrderFullEvent>> orderFullEventTopic = new ArrayList<>();
    static List<ConsumerRecord<String, OrderFullEvent>> valuableCustomerEventTopic = new ArrayList<>();
    static List<ConsumerRecord<String, OrderFullEvent>> halfFullCartEventTopic = new ArrayList<>();
    static List<ConsumerRecord<String, OrderPartialEvent>> fullMiniCartEventTopic = new ArrayList<>();
    static List<ConsumerRecord<String, Error>> errorEventTopic = new ArrayList<>();

    @BeforeEach
    public void clear() {
        orderFullEventTopic.clear();
        valuableCustomerEventTopic.clear();
        halfFullCartEventTopic.clear();
        fullMiniCartEventTopic.clear();
        errorEventTopic.clear();
    }

    @SuppressWarnings("unchecked")
    @KafkaListener(
            topics = {"${com.mario.kafka.order-full-topic}",
                      "${com.mario.kafka.valuable-customer}",
                      "${com.mario.kafka.half-full-cart}",
                      "${com.mario.kafka.full-mini-cart}",
                      "error-topic"},
            groupId = "test-transformer-group",
            autoStartup = "true",
            containerFactory = "listenerFactory"
    )
    public void listener(ConsumerRecord<String, ?> consumerRecord) {
        if(consumerRecord.topic().equals(kafkaProperties.getOrderFullTopic())) {
            orderFullEventTopic.add((ConsumerRecord<String, OrderFullEvent>) consumerRecord);
        }

        if(consumerRecord.topic().equals(kafkaProperties.getValuableCustomer())) {
            valuableCustomerEventTopic.add((ConsumerRecord<String, OrderFullEvent>) consumerRecord);
        }

        if(consumerRecord.topic().equals("half-full-cart")) {
            halfFullCartEventTopic.add((ConsumerRecord<String, OrderFullEvent>) consumerRecord);
        }

        if(consumerRecord.topic().equals("full-mini-cart")) {
            fullMiniCartEventTopic.add((ConsumerRecord<String, OrderPartialEvent>) consumerRecord);
        }

        if(consumerRecord.topic().equals("error-topic")) {
            errorEventTopic.add((ConsumerRecord<String, Error>) consumerRecord);
        }
    }


    protected void kafkaSend(String topic, String key, Object value) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
