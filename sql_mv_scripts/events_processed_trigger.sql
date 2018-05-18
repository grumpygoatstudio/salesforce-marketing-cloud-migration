DROP PROCEDURE refresh_events_processed_now; 

DELIMITER $$

CREATE PROCEDURE refresh_events_processed_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE events_processed;
		INSERT INTO events_processed
        
        SELECT 
			e.id AS id,
            e.venue_id AS venue_id,
            e.name AS name,
            e.logo_url AS logo_url,
			CASE WHEN e.name LIKE '%Special Event%' THEN 1 END AS special_event,
            CASE WHEN e.name LIKE '%Presents%' THEN 1 END AS presents_event
		FROM seatengine.events e;

  SET rc = 0;
END;
$$

DELIMITER ;
