package com.mario.transformator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class TransformatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransformatorApplication.class, args);
    }

}
