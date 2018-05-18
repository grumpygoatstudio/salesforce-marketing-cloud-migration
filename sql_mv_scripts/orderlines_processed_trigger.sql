DROP PROCEDURE refresh_orderlines_processed_now; 

DELIMITER $$

CREATE PROCEDURE refresh_orderlines_processed_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE orderlines_processed;
		INSERT INTO orderlines_processed
        
        SELECT 
			ol.*
		FROM seatengine.orderlines ol;

  SET rc = 0;
END;
$$

DELIMITER ;
