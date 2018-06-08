DROP PROCEDURE refresh_orders_mv_now;

DELIMITER $$

CREATE PROCEDURE refresh_orders_mv_now (
    OUT rc INT
)
BEGIN
		# turn OFF all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 0; 
		
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
            o.not_comped AS not_comped,
            o.booking_type AS booking_type,
            o.addons AS addons,
            o.new_customer AS new_customer,
            v.id AS venue_id,
            o.sys_entry_date AS sys_entry_date
		FROM seatengine.orders_processed o
		JOIN seatengine.orderlines_processed ol ON (o.order_number = ol.order_number)
		JOIN seatengine.shows_processed s ON (s.id = o.show_id)
		JOIN seatengine.events_processed e ON (e.id = s.event_id)
		JOIN seatengine.venues_processed v ON (v.id = e.venue_id);
        
        # turn ON all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 1; 
        
  SET rc = 0;
END;
$$

DELIMITER ;
