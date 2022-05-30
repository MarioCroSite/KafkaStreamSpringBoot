package com.mario.transformator.service;

import com.mario.events.OrderEvent;
import com.mario.events.OrderFullEvent;
import com.mario.transformator.config.KafkaProperties;
import com.mario.transformator.handlers.OrderFullEventTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaStream {

    @Bean
    public KStream<String, OrderEvent> kStream(StreamsBuilder streamsBuilder, KafkaProperties kafkaProperties) {

        var stringSerde = Serdes.String();
        var orderEventSerde = new JsonSerde<>(OrderEvent.class);
        var orderCalculatedEventSerde = new JsonSerde<>(OrderFullEvent.class);

        var incomingOrderEvent = streamsBuilder
                .stream(kafkaProperties.getOrderTopic(), Consumed.with(stringSerde, orderEventSerde))
                .peek((key, value) -> System.out.println("[TRANSFORMER] Key="+ key +", Value="+ value));

        incomingOrderEvent
                .transformValues(OrderFullEventTransformer::new)
                .to(kafkaProperties.getOrderFullTopic(),
                        Produced.with(stringSerde, orderCalculatedEventSerde));


        return incomingOrderEvent;
    }

}
