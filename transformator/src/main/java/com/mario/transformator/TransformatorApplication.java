package com.mario.transformator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
@EnableJpaRepositories("com.mario.transformator.repositories")
public class TransformatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransformatorApplication.class, args);
    }

}
