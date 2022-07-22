package com.mario.transformator.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Autowired
    KafkaProperties kafkaProperties;

    //START consumerFactory committed
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> readCommittedContainerFactory(/*KafkaTransactionManager<?, ?> ktm*/) {
        final DefaultErrorHandler errorHandler =
                new DefaultErrorHandler((record, exception) -> {
                    // 2 seconds pause, 4 retries.
                }, new FixedBackOff(2000L, 4L));

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testConsumerFactoryReadCommitted(kafkaProperties));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        //factory.setCommonErrorHandler(errorHandler);
        //factory.getContainerProperties().setTransactionManager(ktm);
        return factory;
    }

    private ConsumerFactory<String, Object> testConsumerFactoryReadCommitted(KafkaProperties kafkaProperties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return new DefaultKafkaConsumerFactory<>(config);
    }
    //END consumerFactory committed

    //START consumerFactory uncommitted
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> readUncommittedContainerFactory(/*KafkaTransactionManager<?, ?> ktm*/) {
        final DefaultErrorHandler errorHandler =
                new DefaultErrorHandler((record, exception) -> {
                    // 2 seconds pause, 4 retries.
                }, new FixedBackOff(2000L, 4L));
        final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(testConsumerFactoryReadUncommitted(kafkaProperties));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        //factory.setCommonErrorHandler(errorHandler);
        //factory.getContainerProperties().setTransactionManager(ktm);
        return factory;
    }

    public ConsumerFactory<String, Object> testConsumerFactoryReadUncommitted(KafkaProperties kafkaProperties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
        //https://stackoverflow.com/questions/61884941/kafka-listener-poll-interval-how-to-schedule-kafka-consumer-poll-with-15-m
        return new DefaultKafkaConsumerFactory<>(config);
    }
    //END consumerFactory uncommitted

}
