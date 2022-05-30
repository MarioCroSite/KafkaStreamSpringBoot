package com.mario.events;

import java.math.BigDecimal;

public class Product {
    private String id;
    private String name;
    private BigDecimal price;

    public Product() {
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public static final class ProductBuilder {
        private String id;
        private String name;
        private BigDecimal price;

        public static ProductBuilder aProduct() {
            return new ProductBuilder();
        }

        public ProductBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public ProductBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public ProductBuilder withPrice(BigDecimal price) {
            this.price = price;
            return this;
        }

        public Product build() {
            var product = new Product();
            product.id = this.id;
            product.name = this.name;
            product.price = this.price;
            return product;
        }
    }

    @Override
    public String toString() {
        return "Product{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }

}
