package com.mario.transformer.util;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionalHelper {

//    @Transactional(transactionManager = "transactionManager",
//            propagation = Propagation.REQUIRED)
//    public void executeInTransaction(Function f) {
//        f.perform();
//    }

    @Transactional(transactionManager = "kafkaTransactionManager",
            propagation = Propagation.REQUIRED)
    public void executeInKafkaTransaction(Function f) {
        f.perform();
    }

    public interface Function {
        void perform();
    }

}
