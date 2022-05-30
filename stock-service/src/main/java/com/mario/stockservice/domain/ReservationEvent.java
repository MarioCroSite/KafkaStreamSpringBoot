package com.mario.stockservice.domain;

public class ReservationEvent {
    private int itemsAvailable = 0;
    private int itemsReserved = 0;

    public ReservationEvent() {
    }

    public ReservationEvent(int itemsAvailable) {
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
        return "ReservationEvent{" +
                "itemsAvailable=" + itemsAvailable +
                ", itemsReserved=" + itemsReserved +
                '}';
    }

}
