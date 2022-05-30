package com.mario.paymentservice.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Randomizer {

    public static BigDecimal generate(BigDecimal min, BigDecimal max) {
        return min.add(BigDecimal.valueOf(Math.random())
                .multiply(max.subtract(min)))
                .setScale(2, RoundingMode.HALF_UP);
    }

}
