package com.mario.events;

public class StockReservation {
    private int itemsAvailable;
    private int itemsReserved;

    public StockReservation() {
    }

    public StockReservation(int itemsAvailable) {
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
        return "StockReservation{" +
                "itemsAvailable=" + itemsAvailable +
                ", itemsReserved=" + itemsReserved +
                '}';
    }

}
