package com.mario.transformer.controllers;

import com.mario.events.OrderEvent;
import com.mario.transformer.service.KafkaControllerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaControllerService kafkaControllerService;

    public KafkaController(KafkaControllerService kafkaControllerService) {
        this.kafkaControllerService = kafkaControllerService;
    }

    @PostMapping("/1")
    public void orderEvent1(@RequestBody OrderEvent event) {
        kafkaControllerService.sendToKafkaAndDBScenario1(event);
    }

    @PostMapping("/2")
    public void orderEvent2(@RequestBody OrderEvent event) {
        kafkaControllerService.sendToKafkaAndDBScenario2(event);
    }

    @PostMapping("/3")
    public void orderEvent3(@RequestBody OrderEvent event) {
        kafkaControllerService.sendToKafkaAndDBScenario3(event);
    }

}
