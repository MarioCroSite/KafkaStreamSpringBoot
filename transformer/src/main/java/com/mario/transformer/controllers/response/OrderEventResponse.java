package com.mario.transformer.controllers.response;

import com.mario.events.OrderEvent;

import java.util.List;

public class OrderEventResponse {

    private List<OrderEvent> orderEvents;

    public OrderEventResponse() {
    }

    public OrderEventResponse(List<OrderEvent> orderEvents) {
        this.orderEvents = orderEvents;
    }

    public List<OrderEvent> getOrderEvents() {
        return orderEvents;
    }

}
