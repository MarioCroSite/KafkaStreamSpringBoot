package com.mario.events;

import java.math.BigDecimal;
import java.util.List;

public class OrderFullEvent {
    private String id;
    private String marketId;
    private String customerId;
    private Integer productCount;
    private BigDecimal price;
    private List<Product> products;
    private Status status;
    private Source source;

    public OrderFullEvent() {
    }

    public String getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public Integer getProductCount() {
        return productCount;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public List<Product> getProducts() {
        return products;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getMarketId() {
        return marketId;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public static final class OrderCalculatedEventBuilder {
        private String id;
        private String marketId;
        private String customerId;
        private Integer productCount;
        private BigDecimal price;
        private List<Product> products;
        private Status status;
        private Source source;

        public static OrderCalculatedEventBuilder aOrderCalculatedEvent() {
            return new OrderCalculatedEventBuilder();
        }

        public OrderCalculatedEventBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public OrderCalculatedEventBuilder withMarketId(String marketId) {
            this.marketId = marketId;
            return this;
        }

        public OrderCalculatedEventBuilder withCustomerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public OrderCalculatedEventBuilder withProductCount(Integer productCount) {
            this.productCount = productCount;
            return this;
        }

        public OrderCalculatedEventBuilder withPrice(BigDecimal price) {
            this.price = price;
            return this;
        }

        public OrderCalculatedEventBuilder withProducts(List<Product> products) {
            this.products = products;
            return this;
        }

        public OrderCalculatedEventBuilder withStatus(Status status) {
            this.status = status;
            return this;
        }

        public OrderCalculatedEventBuilder withSource(Source source) {
            this.source = source;
            return this;
        }

        public OrderFullEvent build() {
            var event = new OrderFullEvent();
            event.id = this.id;
            event.marketId = this.marketId;
            event.customerId = this.customerId;
            event.productCount = this.productCount;
            event.price = this.price;
            event.products = this.products;
            event.status = this.status;
            event.source = this.source;
            return event;
        }
    }

    @Override
    public String toString() {
        return "OrderFullEvent{" +
                "id='" + id + '\'' +
                ", marketId='" + marketId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productCount=" + productCount +
                ", price=" + price +
                ", products=" + products +
                ", status=" + status +
                ", source=" + source +
                '}';
    }

}
