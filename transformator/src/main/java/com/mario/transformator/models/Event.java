package com.mario.transformator.models;

import com.mario.events.OrderFullEvent;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.List;

@Entity
@Table(name = "event")
public class Event {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "market_id", nullable = false)
    private String marketId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "product_count", nullable = false)
    private Integer productCount;

    @Column(name = "price", nullable = false)
    private BigDecimal price;

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "product_id", nullable = false)
    private List<Product> products;

    public Event() {

    }

    public static Event fromOrderFullEvent(OrderFullEvent orderFullEvent) {
        Event event = new Event();
        event.setMarketId(orderFullEvent.getMarketId());
        event.setCustomerId(orderFullEvent.getCustomerId());
        event.setProductCount(orderFullEvent.getProductCount());
        event.setPrice(orderFullEvent.getPrice());

        orderFullEvent.getProducts().forEach(product -> {
            Product pro = new Product();
            pro.setPrice(product.getPrice());
            pro.setName(product.getName());
            event.getProducts().add(pro);
        });

        return event;
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMarketId() {
        return marketId;
    }

    public void setMarketId(String marketId) {
        this.marketId = marketId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Integer getProductCount() {
        return productCount;
    }

    public void setProductCount(Integer productCount) {
        this.productCount = productCount;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }

}
