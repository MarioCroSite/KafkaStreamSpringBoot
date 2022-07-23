package com.mario.transformator.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.transaction.ChainedTransactionManager;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import javax.persistence.EntityManagerFactory;
import javax.transaction.SystemException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    public static HostInfo hostInfo = new HostInfo("localhost", 8082);

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(KafkaProperties kafkaProperties) {
        var properties = new HashMap<String, Object>();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getApplicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo.host() + ":" + hostInfo.port());

        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> listenerFactory(KafkaProperties kafkaProperties /*, KafkaTransactionManager<?, ?> ktm*/) {
        final DefaultErrorHandler errorHandler =
                new DefaultErrorHandler((record, exception) -> {
                    // 2 seconds pause, 4 retries.
                }, new FixedBackOff(2000L, 4L));

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaEventsConsumerFactory(kafkaProperties));
        factory.setCommonErrorHandler(errorHandler);
        //factory.getContainerProperties().setTransactionManager(ktm);
        //https://stackoverflow.com/questions/47354521/transaction-synchronization-in-spring-kafka
        return factory;
    }

    public ConsumerFactory<String, Object> kafkaEventsConsumerFactory(KafkaProperties kafkaProperties) {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT)); //by default IsolationLevel.READ_UNCOMMITTED
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, kafkaProperties.getTrustedPackages());
        return new DefaultKafkaConsumerFactory<>(props);
    }


    @Bean
    public KafkaTransactionManager<?, ?> kafkaTransactionManager(final ProducerFactory<String, String> producerFactoryTransactional) {
        KafkaTransactionManager<?, ?> manager = new KafkaTransactionManager<>(producerFactoryTransactional);

        manager.setFailEarlyOnGlobalRollbackOnly(true);
        manager.setNestedTransactionAllowed(true);
        manager.setValidateExistingTransaction(true);
        manager.setRollbackOnCommitFailure(true);

        manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ALWAYS);
        return manager;
    }

    @Bean
    public JpaTransactionManager transactionManager(EntityManagerFactory em) {
        JpaTransactionManager manager = new JpaTransactionManager(em);

        manager.setFailEarlyOnGlobalRollbackOnly(true);
        manager.setNestedTransactionAllowed(true);
        manager.setValidateExistingTransaction(true);
        manager.setRollbackOnCommitFailure(true);

        return manager;
    }

    //https://blog.devgenius.io/transactional-integration-kafka-with-database-7eb5fc270bdc
    //https://github.com/spring-projects/spring-kafka/issues/1699
    //https://dzone.com/articles/most-common-spring-transactional-mistakes
    //https://stackoverflow.com/questions/33202841/difference-between-jtatransactionmanager-and-chainedtransactionmanager
    //https://www.slideshare.net/bibryam/dual-write-strategies-for-microservices
    //https://stackoverflow.com/questions/52207598/synchronising-transactions-between-database-and-kafka-producer
    //https://stackoverflow.com/questions/47354521/transaction-synchronization-in-spring-kafka
    //https://grapeup.com/blog/kafka-transactions-integrating-with-legacy-systems/#
    //https://stackoverflow.com/questions/47354521/transaction-synchronization-in-spring-kafka
    //https://github.com/spring-projects/spring-kafka/issues/433
    //https://github.com/spring-projects/spring-kafka/issues/489 //Kafka does not support XA transactions
    //https://stackoverflow.com/questions/58128037/does-kafka-supports-xa-transactions

//    @Bean(name = "chainedTransactionManager")
//    public ChainedTransactionManager chainedTransactionManager(JpaTransactionManager jpaTransactionManager,
//                                                               KafkaTransactionManager<?, ?> kafkaTransactionManager) {
//        //first wil be executed jpaTransactionManager then kafkaTransactionManager
//        return new ChainedTransactionManager(kafkaTransactionManager, jpaTransactionManager);
//
//        //first wil be executed kafkaTransactionManager then jpaTransactionManager
//        //return new ChainedTransactionManager(jpaTransactionManager, kafkaTransactionManager);
//    }

//    @Bean
//    public UserTransactionManager atomikosTransactionManager() throws SystemException {
//        UserTransactionManager userTransactionManager = new UserTransactionManager();
//        userTransactionManager.setTransactionTimeout(600000);
//        return userTransactionManager;
//    }
//
//    @Bean
//    public JtaTransactionManager jtaTransactionManager(UserTransactionManager atomikosTransactionManager) {
//        JtaTransactionManager transactionManager = new JtaTransactionManager();
//        transactionManager.setTransactionManager(atomikosTransactionManager);
//        transactionManager.setUserTransaction(atomikosTransactionManager);
//        return transactionManager;
//    }

    @Bean
    @Qualifier("nonTransactional")
    public KafkaTemplate<String, String> kafkaTemplateNonTransactional(final ProducerFactory<String, String> producerFactoryNonTransactional) {
        return new KafkaTemplate<>(producerFactoryNonTransactional);
    }

    @Bean
    @Qualifier("transactional")
    public KafkaTemplate<String, String> kafkaTemplateTransactional(final ProducerFactory<String, String> producerFactoryTransactional) {
        return new KafkaTemplate<>(producerFactoryTransactional);
    }

    @Bean
    public ProducerFactory<String, String> producerFactoryNonTransactional(KafkaProperties kafkaProperties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public ProducerFactory<String, String> producerFactoryTransactional(KafkaProperties kafkaProperties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transformer-1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //config.put(ProducerConfig.ACKS_CONFIG, "all");
        //config.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(config);
    }

}
