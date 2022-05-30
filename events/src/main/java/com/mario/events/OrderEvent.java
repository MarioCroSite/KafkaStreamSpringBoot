package com.mario.events;

import java.util.List;

public class OrderEvent {
    private String id;
    private String marketId;
    private String customerId;
    private List<Product> products;

    public OrderEvent() {
    }

    public String getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public List<Product> getProducts() {
        return products;
    }

    public String getMarketId() {
        return marketId;
    }

    public static final class OrderEventBuilder {
        private String id;
        private String marketId;
        private String customerId;
        private List<Product> products;

        public static OrderEventBuilder aOrderEvent() {
            return new OrderEventBuilder();
        }

        public OrderEventBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public OrderEventBuilder withMarketId(String marketId) {
            this.marketId = marketId;
            return this;
        }

        public OrderEventBuilder withCustomerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public OrderEventBuilder withProducts(List<Product> products) {
            this.products = products;
            return this;
        }

        public OrderEvent build() {
            var event = new OrderEvent();
            event.id = this.id;
            event.marketId = this.marketId;
            event.customerId = this.customerId;
            event.products = this.products;
            return event;
        }
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "id='" + id + '\'' +
                ", marketId='" + marketId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", products=" + products +
                '}';
    }

}
