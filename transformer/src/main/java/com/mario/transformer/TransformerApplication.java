package com.mario.transformer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
public class TransformerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransformerApplication.class, args);
    }

}
