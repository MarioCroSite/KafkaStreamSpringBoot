package com.mario.stockservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderFullEvent;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.service.KafkaStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"${com.mario.kafka.order-full-topic}"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
public class TestBase {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    StreamsBuilderFactoryBean streamsFactory;

    TopologyTestDriver testDriver;
    public static List<ConsumerRecord<String, OrderFullEvent>> stockOrdersEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, Error>> errorEventTopic = new ArrayList<>();

    @BeforeEach
    public void clear() {
        stockOrdersEventTopic.clear();
        errorEventTopic.clear();

        testDriver = new TopologyTestDriver(KafkaStream.topology(new StreamsBuilder(), kafkaProperties), dummyProperties());
    }

    private Properties dummyProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }

    @SuppressWarnings("unchecked")
    @KafkaListener(
            topics = {"${com.mario.kafka.stock-orders}", "error-topic"},
            groupId = "test-store-group",
            autoStartup = "true",
            containerFactory = "listenerFactory"
    )
    public void listener(ConsumerRecord<String, ?> consumerRecord) {
        if(consumerRecord.topic().equals(kafkaProperties.getStockOrders())) {
            stockOrdersEventTopic.add((ConsumerRecord<String, OrderFullEvent>) consumerRecord);
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
