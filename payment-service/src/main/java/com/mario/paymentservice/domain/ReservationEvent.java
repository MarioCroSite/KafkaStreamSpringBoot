package com.mario.paymentservice.domain;

import java.math.BigDecimal;

public class ReservationEvent {
    private BigDecimal amountAvailable = BigDecimal.ZERO;
    private BigDecimal amountReserved = BigDecimal.ZERO;

    public ReservationEvent() {
    }

    public ReservationEvent(BigDecimal amountAvailable) {
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
        return "ReservationEvent{" +
                "amountAvailable=" + amountAvailable +
                ", amountReserved=" + amountReserved +
                '}';
    }

}
