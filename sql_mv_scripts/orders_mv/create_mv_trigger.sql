DROP PROCEDURE refresh_orders_mv_now;

DELIMITER $$

CREATE PROCEDURE refresh_orders_mv_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE orders_mv;
		INSERT INTO orders_mv
		SELECT
			ol.id AS unique_id,
			o.id AS externalid,
			o.email AS email,
			o.order_number AS orderNumber,
			o.purchase_date_formatted AS orderDate,
			o.order_total AS totalPrice,
			o.cust_id AS customerid,
			o.payment_method AS shippingMethod,
			ol.ticket_name AS orderproduct_name,
			ol.ticket_price AS orderproduct_price,
			o.show_id AS orderproduct_category,
			o.comped AS comped,
            o.not_comped AS not_comped
		FROM seatengine.orders_processed o
		JOIN seatengine.orderlines ol ON (o.order_number = ol.order_number)
		GROUP BY o.order_number, ol.id;
  SET rc = 0;
END;
$$

DELIMITER ;
