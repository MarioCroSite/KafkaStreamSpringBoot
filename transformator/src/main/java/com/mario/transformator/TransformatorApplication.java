package com.mario.transformator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
public class TransformatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransformatorApplication.class, args);
    }

}
