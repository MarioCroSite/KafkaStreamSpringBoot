package com.mario.events;

import java.math.BigDecimal;
import java.util.List;

public class OrderPartialEvent {
    private String id;
    private Integer productCount;
    private BigDecimal price;
    private List<Product> products;

    public OrderPartialEvent() {
    }

    public String getId() {
        return id;
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

    public static final class OrderPartialEventBuilder {
        private String id;
        private Integer productCount;
        private BigDecimal price;
        private List<Product> products;


        public static OrderPartialEventBuilder aOrderPartialEvent() {
            return new OrderPartialEventBuilder();
        }

        public OrderPartialEventBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public OrderPartialEventBuilder withProductCount(Integer productCount) {
            this.productCount = productCount;
            return this;
        }

        public OrderPartialEventBuilder withPrice(BigDecimal price) {
            this.price = price;
            return this;
        }

        public OrderPartialEventBuilder withProducts(List<Product> products) {
            this.products = products;
            return this;
        }

        public OrderPartialEvent build() {
            var event = new OrderPartialEvent();
            event.id = this.id;
            event.productCount = this.productCount;
            event.price = this.price;
            event.products = this.products;
            return event;
        }
    }

    @Override
    public String toString() {
        return "OrderPartialEvent{" +
                "id='" + id + '\'' +
                ", productCount=" + productCount +
                ", price=" + price +
                ", products=" + products +
                '}';
    }

}
