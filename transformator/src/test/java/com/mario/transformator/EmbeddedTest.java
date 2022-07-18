package com.mario.transformator;

import com.mario.events.*;
import com.mario.transformator.controllers.response.OrderEventResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith({OutputCaptureExtension.class})
class EmbeddedTest extends TestBase {


    @Test
    void processSuccessTopology() {
        var firstInputEvent = TestData.orderEvents().get(0);
        var secondInputEvent = TestData.orderEvents().get(1);
        var thirdInputEvent = TestData.orderEvents().get(2);


        //first event
        kafkaSend(kafkaProperties.getOrderTopic(), firstInputEvent.getId(), firstInputEvent);
        await().until(() -> orderFullEventTopic.size() == 1);
        await().until(() -> fullMiniCartEventTopic.size() == 1);

        var firstOutputEvent = orderFullEventTopic.get(0);
        verifyInputOutputEvent(firstInputEvent, firstOutputEvent.value());

        var fullMiniFirstOutput = fullMiniCartEventTopic.get(0);
        assertEquals(firstInputEvent.getId(), fullMiniFirstOutput.key());
        verifyInputOutputEvent(firstInputEvent, fullMiniFirstOutput.value());


        //second event
        kafkaSend(kafkaProperties.getOrderTopic(), secondInputEvent.getId(), secondInputEvent);
        await().until(() -> orderFullEventTopic.size() == 2);
        await().until(() -> valuableCustomerEventTopic.size() == 1);
        await().until(() -> fullMiniCartEventTopic.size() == 2);

        var secondOutputEvent = orderFullEventTopic.get(1);
        verifyInputOutputEvent(secondInputEvent, secondOutputEvent.value());

        var valuableCustomerFirstOutput = valuableCustomerEventTopic.get(0);
        assertEquals(secondInputEvent.getId(), valuableCustomerFirstOutput.key());
        verifyInputOutputEvent(secondInputEvent, valuableCustomerFirstOutput.value());

        var fullMiniSecondOutput = fullMiniCartEventTopic.get(1);
        assertEquals(secondInputEvent.getId(), fullMiniSecondOutput.key());
        verifyInputOutputEvent(secondInputEvent, fullMiniSecondOutput.value());


        //third event
        kafkaSend(kafkaProperties.getOrderTopic(), thirdInputEvent.getId(), thirdInputEvent);
        await().until(() -> orderFullEventTopic.size() == 3);
        await().until(() -> valuableCustomerEventTopic.size() == 2);
        await().until(() -> halfFullCartEventTopic.size() == 1);

        var thirdOutputEvent = orderFullEventTopic.get(2);
        verifyInputOutputEvent(thirdInputEvent, thirdOutputEvent.value());

        var valuableCustomerSecond = valuableCustomerEventTopic.get(1);
        assertEquals(thirdInputEvent.getId(), valuableCustomerSecond.key());
        verifyInputOutputEvent(thirdInputEvent, valuableCustomerSecond.value());

        var halfFullFirstOutput = halfFullCartEventTopic.get(0);
        assertEquals(thirdInputEvent.getId(), halfFullFirstOutput.key());
        verifyInputOutputEvent(thirdInputEvent, halfFullFirstOutput.value());
    }

    @Test
    void processDeserializationError(CapturedOutput output) {
        kafkaSend(kafkaProperties.getOrderTopic(), UUID.randomUUID().toString(), "test");
        await().until(() -> output.getOut().contains("Exception caught during Deserialization"));

        var firstInputEvent = TestData.orderEvents().get(0);
        kafkaSend(kafkaProperties.getOrderTopic(), firstInputEvent.getId(), firstInputEvent);
        await().until(() -> orderFullEventTopic.size() == 1);
        verifyInputOutputEvent(firstInputEvent, orderFullEventTopic.get(0).value());
    }

    @Test
    void processErrorTopic() {
        var orderEventError = TestData.orderEventError();
        kafkaSend(kafkaProperties.getOrderTopic(), orderEventError.getId(), orderEventError);
        await().until(() -> errorEventTopic.size() == 1);

        var errorResponseTopic = errorEventTopic.get(0);
        assertEquals(errorResponseTopic.key(), orderEventError.getId());
        assertThat(errorResponseTopic.value().toString()).contains("java.lang.Error: / by zero");
    }

    @Test
    void getAllCreatedOrders() throws Exception {
        var events = TestData.orderEvents();

        events.forEach(orderEvent ->
                kafkaSend(kafkaProperties.getOrderTopic(), orderEvent.getId(), orderEvent));

        TimeUnit.SECONDS.sleep(5);

        var result = mockMvc.perform(get("/orders"))
                .andExpect(status().isOk())
                .andReturn();

        var response = fromJson(result.getResponse().getContentAsString(), OrderEventResponse.class);


        events.forEach(input -> {
            var res = response.getOrderEvents().stream()
                    .filter(e -> e.getId().equals(input.getId()))
                    .findFirst().orElseThrow();
            verifyInputOutputEvent(input, res);
        });
    }

    @Test
    void getOneCreatedOrder() throws Exception {
        var events = TestData.orderEvents();

        events.forEach(orderEvent ->
                kafkaSend(kafkaProperties.getOrderTopic(), orderEvent.getId(), orderEvent));

        TimeUnit.SECONDS.sleep(5);

        var result = mockMvc.perform(get("/orders/"+events.get(0).getId()))
                .andExpect(status().isOk())
                .andReturn();

        var response = fromJson(result.getResponse().getContentAsString(), OrderEvent.class);

        verifyInputOutputEvent(events.get(0), response);
    }

    @Test
    void getOrderEventNotFound() throws Exception {
        var events = TestData.orderEvents();

        events.forEach(orderEvent ->
                kafkaSend(kafkaProperties.getOrderTopic(), orderEvent.getId(), orderEvent));

        TimeUnit.SECONDS.sleep(5);

        var result = mockMvc.perform(get("/orders/1"))
                .andExpect(status().isNotFound())
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).contains("OrderEvent with key 1 not found");
    }

    private void verifyInputOutputEvent(OrderEvent input, OrderFullEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProducts().size(), output.getProductCount());
        assertEquals(calculatePrice(input.getProducts()), output.getPrice());
        assertEquals(Status.NEW, output.getStatus());
    }

    private void verifyInputOutputEvent(OrderEvent input, OrderPartialEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getProducts().size(), output.getProductCount());
        assertEquals(calculatePrice(input.getProducts()), output.getPrice());
    }

    private void verifyInputOutputEvent(OrderEvent input, OrderEvent output) {
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getMarketId(), output.getMarketId());
        assertEquals(input.getCustomerId(), output.getCustomerId());
        assertEquals(input.getProducts().size(), output.getProducts().size());
    }

    private BigDecimal calculatePrice(List<Product> products) {
        return products.stream().map(Product::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

}
