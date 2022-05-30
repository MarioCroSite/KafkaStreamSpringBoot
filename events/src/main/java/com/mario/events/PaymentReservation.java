package com.mario.events;

import java.math.BigDecimal;

public class PaymentReservation {
    private BigDecimal amountAvailable;
    private BigDecimal amountReserved;

    public PaymentReservation() {
    }

    public PaymentReservation(BigDecimal amountAvailable) {
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
        return "PaymentReservation{" +
                "amountAvailable=" + amountAvailable +
                ", amountReserved=" + amountReserved +
                '}';
    }

}
