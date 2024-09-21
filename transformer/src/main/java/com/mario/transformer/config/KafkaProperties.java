package com.mario.transformer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "com.mario.kafka")
public class KafkaProperties {
    private String orderTopic;
    private String orderFullTopic;
    private String valuableCustomer;
    private String applicationId;
    private List<String> bootstrapServers;
    private String securityProtocol;
    private String consumerGroupId;
    private String trustedPackages;
    private BigDecimal valuableCustomerThreshold;
    private String apiEndpoint;

    public String getOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(String orderTopic) {
        this.orderTopic = orderTopic;
    }

    public String getOrderFullTopic() {
        return orderFullTopic;
    }

    public void setOrderFullTopic(String orderFullTopic) {
        this.orderFullTopic = orderFullTopic;
    }

    public String getValuableCustomer() {
        return valuableCustomer;
    }

    public void setValuableCustomer(String valuableCustomer) {
        this.valuableCustomer = valuableCustomer;
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

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public String getTrustedPackages() {
        return trustedPackages;
    }

    public void setTrustedPackages(String trustedPackages) {
        this.trustedPackages = trustedPackages;
    }

    public BigDecimal getValuableCustomerThreshold() {
        return valuableCustomerThreshold;
    }

    public void setValuableCustomerThreshold(BigDecimal valuableCustomerThreshold) {
        this.valuableCustomerThreshold = valuableCustomerThreshold;
    }

    public String getApiEndpoint() {
        return apiEndpoint;
    }

    public void setApiEndpoint(String apiEndpoint) {
        this.apiEndpoint = apiEndpoint;
    }

    @Override
    public String toString() {
        return "KafkaProperties{" +
                "orderTopic='" + orderTopic + '\'' +
                ", orderFullTopic='" + orderFullTopic + '\'' +
                ", valuableCustomer='" + valuableCustomer + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", bootstrapServers=" + bootstrapServers +
                ", securityProtocol='" + securityProtocol + '\'' +
                ", consumerGroupId='" + consumerGroupId + '\'' +
                ", trustedPackages='" + trustedPackages + '\'' +
                ", valuableCustomerThreshold=" + valuableCustomerThreshold +
                ", apiEndpoint='" + apiEndpoint + '\'' +
                '}';
    }

}
