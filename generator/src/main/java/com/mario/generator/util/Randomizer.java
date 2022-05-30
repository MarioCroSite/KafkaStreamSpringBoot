package com.mario.generator.util;

import com.mario.events.Product;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class Randomizer {

    public static int generate(int min, int max) {
        return min + (int)(Math.random() * ((max - min) + 1));
    }

    public static BigDecimal generate(BigDecimal min, BigDecimal max) {
        return min.add(BigDecimal.valueOf(Math.random())
                .multiply(max.subtract(min)))
                .setScale(2, RoundingMode.HALF_UP);
    }

    public static String getCustomerId() {
        var customerIds = List.of(
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f01",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f02",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f03",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f04",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f05",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f06",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f07",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f08",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f09",
                "f50ec468-f5ec-4ba4-ba60-32b14c3f0f10");

        return customerIds.get(new Random().nextInt(customerIds.size()));
    }

    public static String getMarketId() {
        var marketIds = List.of(
                "30a4496d-ffb5-4235-bbdd-68123d6dcd01",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd02",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd03",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd04",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd05",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd06",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd07",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd08",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd09",
                "30a4496d-ffb5-4235-bbdd-68123d6dcd10");

        return marketIds.get(new Random().nextInt(marketIds.size()));
    }

    public static List<Product> getProducts() {
        List<Product> products = new ArrayList<>();

        IntStream.range(1, generate(1, 20)).forEach(i ->
                products.add(Product.ProductBuilder.aProduct()
                        .withId(UUID.randomUUID().toString())
                        .withPrice(generate(BigDecimal.valueOf(10), BigDecimal.valueOf(5000)))
                        .withName("Product " + generate(1, 20))
                        .build())
        );

        return products;
    }

}
