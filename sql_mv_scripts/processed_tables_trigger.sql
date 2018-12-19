DROP PROCEDURE refresh_processed_tables_now; 

DELIMITER $$

CREATE PROCEDURE refresh_processed_tables_now (
    OUT rc INT
)
BEGIN
		# turn OFF all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 0; 
        
		# truncate all tables
        TRUNCATE TABLE orderlines_processed;
		TRUNCATE TABLE orders_processed;
		TRUNCATE TABLE shows_processed;
		TRUNCATE TABLE events_processed;

        # update orderlines 
        INSERT INTO orderlines_processed
		SELECT 
			ol.id AS id,
            ol.order_number AS order_number,
            ol.ticket_name AS ticket_name,
            ol.ticket_price AS ticket_price,
            ol.printed AS printed,
            ol.promo_code_id AS promo_code_id,
            ol.checked_in AS checked_in
		FROM seatengine.orderlines ol;
        
		# update orders 
        INSERT INTO orders_processed
        SELECT 
			o.id AS id,
            o.show_id AS show_id,
            o.order_number AS order_number,
            o.cust_id AS cust_id,
            o.email AS email,
            o.phone AS phone,
            o.purchase_date AS purchase_date,
            o.payment_method AS payment_method,
            o.booking_type AS booking_type,
            o.order_total AS order_total,
            o.new_customer AS new_customer,
            o.sys_entry_date AS sys_entry_date,
            o.addons AS addons,
            DATE_FORMAT(purchase_date, '%Y-%m-%d') AS purchase_date_formatted,
			CASE WHEN o.payment_method LIKE '%comp%' THEN 1 ELSE 0 END AS comped,
            CASE WHEN o.payment_method NOT LIKE '%comp%' THEN 1 ELSE 0 END AS not_comped
		FROM seatengine.orders o;
        
        # update shows
        INSERT INTO shows_processed
        SELECT 
			s.id AS id,
            s.event_id AS event_id,
            s.start_date_time AS start_date_time,
            s.sold_out AS sold_out,
            s.cancelled_at AS cancelled_at,
			DATE_FORMAT(s.start_date_time, '%Y-%m-%d') AS start_date_formatted,
			WEEKDAY(DATE_FORMAT(s.start_date_time, '%Y-%m-%d')) AS day_of_week
		FROM seatengine.shows s;
        
         # update events
        INSERT INTO events_processed
        SELECT 
			e.id AS id,
            e.venue_id AS venue_id,
            e.name AS name,
            e.logo_url AS logo_url,
			CASE WHEN e.name LIKE '%Special Event%' THEN 1 END AS special_event,
            CASE WHEN e.name LIKE '%Presents%' THEN 1 END AS presents_event
		FROM seatengine.events e;
        
        # turn ON all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 1; 
        
        # call triggers to update final two MVs
        CALL refresh_orders_mv_now(@rc);
        CALL refresh_calculation_tables_now(@rc);
        CALL refresh_contacts_mv_now(@rc);
        CALL refresh_contacts_mobile_mv(@rc);

  SET rc = 0;
END;
$$

DELIMITER ;
