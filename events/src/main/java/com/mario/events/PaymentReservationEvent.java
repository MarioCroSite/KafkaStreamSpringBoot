package com.mario.events;

import java.math.BigDecimal;

public class PaymentReservationEvent {
    private BigDecimal amountAvailable = BigDecimal.ZERO;
    private BigDecimal amountReserved = BigDecimal.ZERO;

    public PaymentReservationEvent() {
    }

    public PaymentReservationEvent(BigDecimal amountAvailable) {
        this.amountAvailable = amountAvailable;
    }

    public BigDecimal getAmountAvailable() {
        return amountAvailable;
    }

    public BigDecimal getAmountReserved() {
        return amountReserved;
    }

    public void setAmountAvailable(BigDecimal amountAvailable) {
        this.amountAvailable = amountAvailable;
    }

    public void setAmountReserved(BigDecimal amountReserved) {
        this.amountReserved = amountReserved;
    }


    @Override
    public String toString() {
        return "PaymentReservationEvent{" +
                "amountAvailable=" + amountAvailable +
                ", amountReserved=" + amountReserved +
                '}';
    }

}
