CREATE TABLE product
(
    id                  SERIAL             NOT NULL,
    name                VARCHAR(255)       NOT NULL,
    price               NUMERIC            NOT NULL,

    CONSTRAINT pk_product PRIMARY KEY(id)
);

CREATE TABLE event
(
    id                  SERIAL             NOT NULL,
    market_id           VARCHAR(255)       NOT NULL,
    customer_id         VARCHAR(255)       NOT NULL,
    product_count       INTEGER            NOT NULL,
    price               NUMERIC            NOT NULL,
    product_id          SERIAL             NOT NULL,

    CONSTRAINT pk_event PRIMARY KEY(id),
    CONSTRAINT fk_event_product FOREIGN KEY (product_id) REFERENCES product(id)
);
