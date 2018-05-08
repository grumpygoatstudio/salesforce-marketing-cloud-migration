DROP TABLE orders_mv;
CREATE TABLE orders_mv (
    unique_id   VARCHAR(255)    NOT NULL,
	externalid   VARCHAR(255)    NOT NULL,
	email   VARCHAR(255)    NOT NULL,
	orderNumber   VARCHAR(255)    NOT NULL,
	orderDate   VARCHAR(255)    NOT NULL,
	totalPrice   VARCHAR(255)    NOT NULL,
	customerid   VARCHAR(255)    NOT NULL,
	shippingMethod   VARCHAR(255)    NOT NULL,
	orderproduct_name   VARCHAR(255)    NOT NULL,
	orderproduct_price   VARCHAR(255)    NOT NULL,
	orderproduct_category   VARCHAR(255)    NOT NULL
);
