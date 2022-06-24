package com.mario.events;

public class StockReservationEvent {
    private int itemsAvailable = 0;
    private int itemsReserved = 0;

    public StockReservationEvent() {
    }

    public StockReservationEvent(int itemsAvailable) {
        this.itemsAvailable = itemsAvailable;
    }

    public int getItemsAvailable() {
        return itemsAvailable;
    }

    public void setItemsAvailable(int itemsAvailable) {
        this.itemsAvailable = itemsAvailable;
    }

    public int getItemsReserved() {
        return itemsReserved;
    }

    public void setItemsReserved(int itemsReserved) {
        this.itemsReserved = itemsReserved;
    }

    @Override
    public String toString() {
        return "StockReservationEvent{" +
                "itemsAvailable=" + itemsAvailable +
                ", itemsReserved=" + itemsReserved +
                '}';
    }

}
