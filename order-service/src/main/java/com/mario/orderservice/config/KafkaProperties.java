package com.mario.orderservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "com.mario.kafka")
public class KafkaProperties {
    private String orderTopic;
    private String orderCalculatedTopic;
    private String applicationId;
    private List<String> bootstrapServers;
    private String securityProtocol;


    public String getOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(String orderTopic) {
        this.orderTopic = orderTopic;
    }

    public String getOrderCalculatedTopic() {
        return orderCalculatedTopic;
    }

    public void setOrderCalculatedTopic(String orderCalculatedTopic) {
        this.orderCalculatedTopic = orderCalculatedTopic;
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
                "orderTopic='" + orderTopic + '\'' +
                ", orderCalculatedTopic='" + orderCalculatedTopic + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", bootstrapServers=" + bootstrapServers +
                ", securityProtocol='" + securityProtocol + '\'' +
                '}';
    }

}