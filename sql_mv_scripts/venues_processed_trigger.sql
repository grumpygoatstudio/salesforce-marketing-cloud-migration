DROP PROCEDURE refresh_venues_processed_now; 

DELIMITER $$

CREATE PROCEDURE refresh_venues_processed_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE venues_processed;
		INSERT INTO venues_processed
        
        SELECT 
			v.*
		FROM seatengine.venues v;

  SET rc = 0;
END;
$$

DELIMITER ;
