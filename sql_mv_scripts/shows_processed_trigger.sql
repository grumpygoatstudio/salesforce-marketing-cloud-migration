DROP PROCEDURE refresh_shows_processed_now; 

DELIMITER $$

CREATE PROCEDURE refresh_shows_processed_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE shows_processed;
		INSERT INTO shows_processed
        
        SELECT 
			s.*,
			DATE_FORMAT(s.start_date_time, '%Y-%m-%d') AS start_date_formatted,
			WEEKDAY(DATE_FORMAT(s.start_date_time, '%Y-%m-%d')) AS day_of_week
		FROM seatengine.shows s;

  SET rc = 0;
END;
$$

DELIMITER ;
