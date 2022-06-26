package com.mario.orderservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "com.mario.kafka")
public class KafkaProperties {
    private String paymentOrders;
    private String stockOrders;
    private String orderFullTopic;
    private String applicationId;
    private List<String> bootstrapServers;
    private String securityProtocol;

    public String getPaymentOrders() {
        return paymentOrders;
    }

    public void setPaymentOrders(String paymentOrders) {
        this.paymentOrders = paymentOrders;
    }

    public String getStockOrders() {
        return stockOrders;
    }

    public void setStockOrders(String stockOrders) {
        this.stockOrders = stockOrders;
    }

    public String getOrderFullTopic() {
        return orderFullTopic;
    }

    public void setOrderFullTopic(String orderFullTopic) {
        this.orderFullTopic = orderFullTopic;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    @Override
    public String toString() {
        return "KafkaProperties{" +
                "paymentOrders='" + paymentOrders + '\'' +
                ", stockOrders='" + stockOrders + '\'' +
                ", orderFullTopic='" + orderFullTopic + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", bootstrapServers=" + bootstrapServers +
                ", securityProtocol='" + securityProtocol + '\'' +
                '}';
    }

}
