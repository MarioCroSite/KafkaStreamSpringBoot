# Event-Driven Spring Boot apps with Kafka Streams
The idea of this project is to demonstrate communication between multiple Spring Boot applications using Kafka Streams.

### Story behind this project
With this project, we want to demonstrate the purchase of products via the webshop.

Imaginary preliminary steps:
1. The user registered on the webshop and decided to buy several products.
2. The user has decided to transfer money on the webshop that he is ready to spend.
3. After the user has added products to the cart, he has decided to make a purchase.
4. After clicking "Buy Now" flow starts

## Key Components
• Events module contains pojo event classes through which information is distributed between different Spring Boot applications

• Generator module serves as a source from which information is sent to the Kafka topic

• Transformer module serves as a central place where messages arrive. Transformation and redirection of messages to other Kafka topics is done via Kafka Streams

• Payment service module is used to check the amount that the user has on the account and reserve money when purchasing products

• Stock service module is used to check the status of product in the warehouse and product reservations when purchasing

• Order service module is used to check the status of the payment service and the stock service. Depending on the status of the customer, the purchase is successfully processed or not processed


## Communication between components
![KafkaStream.png](art/KafkaStream.png)

### Business logic behind communication
Generator module imitates the user's purchase of a products every 5 seconds. OrderEvent is sent to the topic "orders".

Transformer service transforms the OrderEvent into an OrderFullEvent. In other words, it enriches the new "OrderFullEvent" event with information on the total number of products and the final price.
The status of the OrderFullEvent is set to NEW. OrderFullEvent is sent to the topic "orders-full".
- If the user has spent more than 4000€ on a purchase, information about the user and the purchase is automatically sent to the topic "valuable-customer". For example, we can send newsletters for more expensive products to such users in the future
- If the user has bought more than 10 products, information about the user and the product is sent to the topic "full-mini-cart"
- If the user has purchased more than 5 products, information about the user and the product is sent to the topic "half-full-cart"

Payment service checks whether the user has sufficient funds to purchase the products.
The RocksDB database is used in the background, where information about the current amount the user has is stored.
If the user has enough money, the money is reserved and the status "ACCEPT" is set, otherwise the status REJECT is set.
OrderFullEvent with newly set status is sent to topic "payment-orders".

The stock service checks whether there are enough products in stock.
The RocksDB database is used in the background, where product inventory data is stored.
If there are enough products in stock, the products are reserved and the status "ACCEPT" is set, otherwise the status REJECT is set.
OrderFullEvent with newly set status is sent to topic "stock-orders".

Order Service serves as a place that combines events from two different topics and makes a decision whether the order will be processed successfully or not.
If we receive the status ACCEPT from the payment service and the stock service, the order will be successful.
If we receive a REJECT status from the payment service and stock service, the order will be unsuccessful.
If we receive the status ACCEPT from one service, and REJECT from another service, in that case the Order service sets the status Rollback.
Rollback status means that the reservation will be canceled, that is, the reserved money will be returned to the account and the reserved products will be returned to the stock.


## Appendix (Synchronising transactions between database and Apache Kafka producer)

### Story
If until now we have developed an application without a message queue where we only used a database and we need to add Apache Kafka, we can easily have unexpected errors.
If we encounter Apache Kafka for the first time, we will not even think that the database is a separate transaction, and the Apache Kafka producer is another separate transaction.
As the simplest solution, the first thing that comes to our mind is to put the @Transactional annotation above the method, because we used it until now when we wanted to write data in several tables.
After we deploy our application to the staging environment and start intensive tests, it can easily happen that the information is saved in the database but not sent to the Kafka Broker or that the information is saved in the Kafka broker but not written to the database.

### Idea
After researching how to fix these errors, one of the most popular ways is to use the Outbox pattern. 
But if we don't have time to implement this pattern and we need to go into production as soon as possible, there is also a temporary solution.

### Transactions

Creating beans for a Kafka transaction
```java
@Bean
public KafkaTransactionManager<?, ?> kafkaTransactionManager(final ProducerFactory<String, String> producerFactoryTransactional) {
    KafkaTransactionManager<?, ?> manager = new KafkaTransactionManager<>(producerFactoryTransactional);
    manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ALWAYS);
    return manager;
}   
```
Creating beans for a JPA transaction
```java
@Bean
public JpaTransactionManager transactionManager(EntityManagerFactory em) {
    JpaTransactionManager manager = new JpaTransactionManager(em);
    return manager;
}
```

Creating beans for ChainedTransactionManager, i.e. executing first one transaction and then another.
```java
@Bean(name = "chainedTransactionManager")
public ChainedTransactionManager chainedTransactionManager(JpaTransactionManager jpaTransactionManager,
                                                           KafkaTransactionManager<?, ?> kafkaTransactionManager) {
    //first wil be executed jpaTransactionManager then kafkaTransactionManager
    //return new ChainedTransactionManager(kafkaTransactionManager, jpaTransactionManager);

    //first wil be executed kafkaTransactionManager then jpaTransactionManager
    return new ChainedTransactionManager(jpaTransactionManager, kafkaTransactionManager);
}
```

### Approaches
How does the ChainedTransactionManager work?
```text
transaction1 begin
    transaction2 begin
    transaction2 commit -> error rollbacks, rollbacks transction1 too
transaction1 commit -> error, only rollbacks transaction1
```

1. (approach) transaction1 = jpa, transaction2 = kafka
    ```java
    @Transactional(transactionManager = "chainedTransactionManager")
    public void sendToKafkaAndDB(OrderEvent event) {
        eventRepository.save(toEvent(event));
        kafkaSend("tester-1", event.getId(), event);
        eventRepository.save(toEventWithEx(event));
    }
    ```
   In this piece of code, we have a problem in that the Kafka transaction will commit successfully, while the jpa will rollback. 
   With this option, we will have an inconsistent state.

2. (approach) transaction1 = kafka, transaction2 = jpa
   ```java
   @Transactional(transactionManager = "chainedTransactionManager")
   public void sendToKafkaAndDB(OrderEvent event) {
       eventRepository.save(toEvent(event));
       kafkaSend("tester-1", event.getId(), event);
       eventRepository.save(toEventWithEx(event));
   }
    ```
   With this method, since the error is with jpa, jpa will be rollback successfully, while kafka will rollback automatically.
   With this approach, the state will be consistent.
    















