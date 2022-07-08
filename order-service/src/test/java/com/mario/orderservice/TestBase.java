package com.mario.orderservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderFullEvent;
import com.mario.orderservice.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"${com.mario.kafka.payment-orders}", "${com.mario.kafka.stock-orders}"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
public class TestBase {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaTemplate<String, String> kafkaStringTemplate;

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    StreamsBuilderFactoryBean streamsFactory;

    public static List<ConsumerRecord<String, OrderFullEvent>> orderFullEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, Error>> errorEventTopic = new ArrayList<>();

    @BeforeEach
    public void clear() {
        orderFullEventTopic.clear();
        errorEventTopic.clear();
    }

    @SuppressWarnings("unchecked")
    @KafkaListener(
            topics = {"${com.mario.kafka.order-full-topic}", "error-topic"},
            groupId = "test-order-service-group",
            autoStartup = "true",
            containerFactory = "listenerFactory"
    )
    public void listener(ConsumerRecord<String, ?> consumerRecord) {
        if(consumerRecord.topic().equals(kafkaProperties.getOrderFullTopic())) {
            orderFullEventTopic.add((ConsumerRecord<String, OrderFullEvent>) consumerRecord);
        }

        if(consumerRecord.topic().equals("error-topic")) {
            errorEventTopic.add((ConsumerRecord<String, Error>) consumerRecord);
        }
    }


    protected void kafkaSend(String topic, String key, Object value) {
        kafkaTemplate.send(topic, key, value);
    }

}
