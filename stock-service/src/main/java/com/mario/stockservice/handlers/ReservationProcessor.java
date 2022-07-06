package com.mario.stockservice.handlers;

import com.mario.events.OrderFullEvent;
import com.mario.events.Source;
import com.mario.events.Status;
import com.mario.events.StockReservationEvent;
import com.mario.pojo.ExecutionResult;
import com.mario.stockservice.config.KafkaProperties;
import com.mario.stockservice.util.MarketUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.core.KafkaTemplate;

public class ReservationProcessor implements ValueTransformerWithKey<String, OrderFullEvent, ExecutionResult<OrderFullEvent>> {

    private final String storeName;
    private KeyValueStore<String, StockReservationEvent> stateStore;
    //private final KafkaTemplate<String, Object> kafkaTemplate;
    //private final KafkaProperties kafkaProperties;

    public ReservationProcessor(String storeName
                                /*KafkaTemplate<String, Object> kafkaTemplate,
                                KafkaProperties kafkaProperties*/) {
        this.storeName = storeName;
        //this.kafkaTemplate = kafkaTemplate;
        //this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void init(ProcessorContext context) {
        stateStore = context.getStateStore(storeName);
    }

    @Override
    public ExecutionResult<OrderFullEvent> transform(String key, OrderFullEvent orderEvent) {
        var reservation = getStateStore(key);

        try {
            switch (orderEvent.getStatus()) {
                case CONFIRMED:
                    reservation.setItemsReserved(reservation.getItemsReserved() - orderEvent.getProductCount());
                    break;
                case ROLLBACK:
                    if(orderEvent.getSource() != null && orderEvent.getSource().equals(Source.STOCK)) {
                        reservation.setItemsAvailable(reservation.getItemsAvailable() + orderEvent.getProductCount());
                        reservation.setItemsReserved(reservation.getItemsReserved() - orderEvent.getProductCount());
                    }
                    break;
                case NEW:
                    if(orderEvent.getProductCount() <= reservation.getItemsAvailable()) {
                        reservation.setItemsAvailable(reservation.getItemsAvailable() - orderEvent.getProductCount());
                        reservation.setItemsReserved(reservation.getItemsReserved() + orderEvent.getProductCount());
                        orderEvent.setStatus(Status.ACCEPT);
                    } else {
                        orderEvent.setStatus(Status.REJECT);
                    }

                    //kafkaTemplate.send(kafkaProperties.getStockOrders(), orderEvent.getId(), orderEvent);
            }
            if(orderEvent.getProductCount() >= 2000){
                throw new RuntimeException("Product Count is too high");
            }
            addStateStore(key, reservation);
            return ExecutionResult.success(orderEvent);
        } catch (Exception e) {
            return ExecutionResult.error(new Error(e.getMessage()));
        }
    }

    private StockReservationEvent getStateStore(String key) {
        var reservation = stateStore.get(key);
        if(reservation == null) {
            reservation = new StockReservationEvent(MarketUtils.MARKET_AVAILABLE_ITEMS);
        }
        return reservation;
    }

    private void addStateStore(String key, StockReservationEvent value) {
        stateStore.put(key, value);
    }

    @Override
    public void close() {

    }

}
