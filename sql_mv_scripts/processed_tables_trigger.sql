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
		SELECT ol.*
		FROM seatengine.orderlines ol;
        
		# update orders 
        INSERT INTO orders_processed
        SELECT o.*,
            DATE_FORMAT(purchase_date, '%Y-%m-%d') AS purchase_date_formatted,
			CASE WHEN o.payment_method LIKE '%comp%' THEN 1 ELSE 0 END AS comped,
            CASE WHEN o.payment_method NOT LIKE '%comp%' THEN 1 ELSE 0 END AS not_comped
		FROM seatengine.orders o;
        
        # update shows
        INSERT INTO shows_processed
        SELECT s.*,
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

  SET rc = 0;
END;
$$

DELIMITER ;
