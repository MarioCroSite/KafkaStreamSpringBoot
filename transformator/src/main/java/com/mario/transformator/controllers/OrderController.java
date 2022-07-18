package com.mario.transformator.controllers;

import com.mario.events.OrderEvent;
import com.mario.transformator.controllers.response.OrderEventResponse;
import com.mario.transformator.exception.ResourceNotFoundException;
import com.mario.transformator.service.KafkaStream;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Objects;

import static java.lang.String.format;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private StreamsBuilderFactoryBean kafkaStreamsFactory;


    public OrderController(StreamsBuilderFactoryBean kafkaStreamsFactory) {
        this.kafkaStreamsFactory = kafkaStreamsFactory;
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
