package com.mario.transformator.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mario.events.OrderEvent;
import com.mario.transformator.config.KafkaConfiguration;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.controllers.response.OrderEventResponse;
import com.mario.transformator.exception.ResourceNotFoundException;
import com.mario.transformator.service.KafkaStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Objects;

import static java.lang.String.format;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private static final Logger logger = LoggerFactory.getLogger(OrderController.class);

    private StreamsBuilderFactoryBean kafkaStreamsFactory;
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, String> kafkaTemplateTx;
    private KafkaProperties kafkaProperties;
    private ObjectMapper objectMapper;

    public OrderController(StreamsBuilderFactoryBean kafkaStreamsFactory,
                           @Qualifier("nonTransactional") KafkaTemplate<String, String> kafkaTemplate,
                           @Qualifier("transactional") KafkaTemplate<String, String> kafkaTemplateTx,
                           KafkaProperties kafkaProperties,
                           ObjectMapper objectMapper) {
        this.kafkaStreamsFactory = kafkaStreamsFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplateTx = kafkaTemplateTx;
        this.kafkaProperties = kafkaProperties;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public OrderEvent orderEvent(@RequestBody OrderEvent event) throws Exception {
        kafkaTemplate.send(kafkaProperties.getOrderTopic(), event.getId(), objectMapper.writeValueAsString(event));
        return event;
    }

    @GetMapping
    public OrderEventResponse getAllOrderEvents() {
        var orders = new ArrayList<OrderEvent>();
        var keyValueIterator = store().all();
        keyValueIterator.forEachRemaining(kv -> orders.add(kv.value));
        return new OrderEventResponse(orders);
    }

    @GetMapping("/{key}")
    public OrderEvent getOrderEventByKey(@PathVariable("key") String key) {
        var keyQuery = Objects.requireNonNull(kafkaStreamsFactory.getKafkaStreams())
                .queryMetadataForKey(KafkaStream.STORE_NAME, key, Serdes.String().serializer());
        if(keyQuery.activeHost().equals(KafkaConfiguration.hostInfo)) {
            logger.info("Running on the same instance");
        } else {
            logger.info("Need to query other instance on host {} and port {}", keyQuery.activeHost().host(), keyQuery.activeHost().port());
            //need to make http call http://host:port/orders/key
        }

        var orderEvent =  store().get(key);
        if(orderEvent == null) {
            throw new ResourceNotFoundException(format("OrderEvent with key %s not found.", key));
        }

        return orderEvent;
    }

    private ReadOnlyKeyValueStore<String, OrderEvent> store() {
        return Objects.requireNonNull(kafkaStreamsFactory
                        .getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(KafkaStream.STORE_NAME, QueryableStoreTypes.keyValueStore()));
    }

}
