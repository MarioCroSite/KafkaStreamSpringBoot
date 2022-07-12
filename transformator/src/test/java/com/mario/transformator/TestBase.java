package com.mario.transformator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.mario.events.OrderFullEvent;
import com.mario.events.OrderPartialEvent;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.service.KafkaStream;
import com.mario.transformator.util.WiremockScenario;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;

@SpringBootTest
@EmbeddedKafka(
        count = 3,
        topics = {"${com.mario.kafka.order-topic}", "topic-test-1", "topic-test-2"}
)
@AutoConfigureWireMock(port=0)
public class TestBase {

    @Autowired
    @Qualifier("nonTransactional")
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    @Qualifier("transactional")
    KafkaTemplate<String, String> kafkaTemplateTx;

    @Autowired
    public KafkaProperties kafkaProperties;

    @Autowired
    ObjectMapper objectMapper;

    TopologyTestDriver testDriver;
    public static List<ConsumerRecord<String, OrderFullEvent>> orderFullEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, OrderFullEvent>> valuableCustomerEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, OrderFullEvent>> halfFullCartEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, OrderPartialEvent>> fullMiniCartEventTopic = new ArrayList<>();
    public static List<ConsumerRecord<String, Error>> errorEventTopic = new ArrayList<>();

    @BeforeEach
    public void init() {
        resetWiremock();
        orderFullEventTopic.clear();
        valuableCustomerEventTopic.clear();
        halfFullCartEventTopic.clear();
        fullMiniCartEventTopic.clear();
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

    void stubWiremock(String url) {
        stubFor(get(urlEqualTo(url))
                .inScenario("Test Scenario")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(HttpStatus.INTERNAL_SERVER_ERROR.value()))
                .willSetStateTo(WiremockScenario.SUCCESS_SCENARIO));

        stubFor(get(urlEqualTo(url))
                .inScenario("Test Scenario")
                .whenScenarioStateIs(WiremockScenario.SUCCESS_SCENARIO)
                .willReturn(aResponse().withStatus(HttpStatus.OK.value()))
                .willSetStateTo(STARTED));
    }

    void stubWiremock(String url, HttpStatus httpStatusResponse) {
        stubFor(get(urlEqualTo(url))
                .willReturn(aResponse().withStatus(httpStatusResponse.value())));
    }

    void resetWiremock() {
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();
    }

    protected void kafkaSend(String topic, String key, Object value) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
