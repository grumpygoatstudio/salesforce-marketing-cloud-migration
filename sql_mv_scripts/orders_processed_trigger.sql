DROP PROCEDURE refresh_orders_processed_now; 

DELIMITER $$

CREATE PROCEDURE refresh_orders_processed_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE orders_processed;
		INSERT INTO orders_processed
        
        SELECT 
			o.*,
            DATE_FORMAT(purchase_date, '%Y-%m-%d') AS purchase_date_formatted,
			CASE WHEN o.payment_method LIKE '%comp%' THEN 1 ELSE 0 END AS comped,
            CASE WHEN o.payment_method NOT LIKE '%comp%' THEN 1 ELSE 0 END AS not_comped
		FROM seatengine.orders o;

  SET rc = 0;
END;
$$

DELIMITER ;
