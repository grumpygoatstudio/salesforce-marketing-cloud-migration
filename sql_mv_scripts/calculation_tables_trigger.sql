DROP PROCEDURE refresh_calculation_tables_now;

DELIMITER $$

CREATE PROCEDURE refresh_calculation_tables_now (
    OUT rc INT
)
BEGIN
		# turn OFF all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 0; 
        
		# truncate all tables
        TRUNCATE TABLE attended_event_dates;
        TRUNCATE TABLE avg_tickets_per_order;
        TRUNCATE TABLE fln_event_data;
        TRUNCATE TABLE last_360;
        TRUNCATE TABLE per_paid_order;
        TRUNCATE TABLE per_comp_order;
        TRUNCATE TABLE special_shows;
        TRUNCATE TABLE total_spend;
        TRUNCATE TABLE weekday_attendence;
        
		# Table for pulling all the first/last/next dates from
        INSERT INTO attended_event_dates
			SELECT c.subscriber_key AS subscriber_key,
				start_date_formatted,
				e.name AS event_title,
				v.id AS event_venue
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN seatengine.shows_processed s ON (s.id = o.show_id)
			JOIN seatengine.events_processed e ON (e.id = s.event_id)
			JOIN seatengine.venues_processed v ON (v.id = e.venue_id)
		;
        
        # table for joining in all the first/last/next dates
        INSERT INTO fln_event_data
			SELECT c.subscriber_key,
				first_show_attended, -- First Show Attended Date
				first_event_title, -- First Event Title
				first_event_venue, -- First Event Venue
				last_show_attended, -- Last Show Attended Date
				last_event_title, -- Last Event Title
				last_event_venue, -- Last Event Venue
				next_show_attending, -- Next Show Attending Date
				next_event_title, -- Next Event Title	
                next_event_venue -- Next Event Venue
			FROM seatengine.contacts c
			## FIRST SHOW / EVENT INFO PER CUSTOMER
			LEFT JOIN
				(SELECT a1.subscriber_key AS subscriber_key,
					start_date_formatted AS first_show_attended, -- First Show Attended Date
					event_title AS first_event_title, -- First Event Title
					event_venue AS first_event_venue -- First Event Venue
				FROM attended_event_dates a1
				JOIN
					(SELECT subscriber_key, MIN(start_date_formatted) AS MinDateTime
					FROM attended_event_dates
					WHERE start_date_formatted <= NOW()
					GROUP BY subscriber_key) AS groupedtt ON (a1.subscriber_key = groupedtt.subscriber_key AND a1.start_date_formatted = groupedtt.MinDateTime)
				) AS a1 ON (c.subscriber_key = a1.subscriber_key)
			## LAST SHOW / EVENT INFO PER CUSTOMER
			LEFT JOIN
				(SELECT a2.subscriber_key AS subscriber_key,
					start_date_formatted AS last_show_attended, -- First Show Attended Date
					event_title AS last_event_title, -- First Event Title
					event_venue AS last_event_venue -- First Event Venue
				FROM attended_event_dates a2
				JOIN
					(SELECT subscriber_key, MAX(start_date_formatted) AS MaxDateTime
					FROM attended_event_dates
					WHERE start_date_formatted <= NOW()
					GROUP BY subscriber_key) AS groupedtt ON (a2.subscriber_key = groupedtt.subscriber_key AND a2 .start_date_formatted = groupedtt.MaxDateTime)
				) AS a2 ON (c.subscriber_key = a2.subscriber_key)
			## NEXT SHOW / EVENT INFO PER CUSTOMER
			LEFT JOIN
				(SELECT a3.subscriber_key,
					start_date_formatted AS next_show_attending, -- Next Show Attending Date
					event_title AS next_event_title, -- Next Event Title
                    event_venue AS next_event_venue -- Next Event Venue
				FROM attended_event_dates a3
				JOIN
					(SELECT subscriber_key, MIN(start_date_formatted) AS MinDateTime
					FROM attended_event_dates
					WHERE start_date_formatted > NOW()
					GROUP BY subscriber_key) AS groupedtt ON (a3.subscriber_key = groupedtt.subscriber_key AND a3.start_date_formatted = groupedtt.MinDateTime)
				) AS a3 ON (c.subscriber_key = a3.subscriber_key)
			GROUP BY c.subscriber_key
			;


        INSERT INTO avg_tickets_per_order
			SELECT c.subscriber_key,
				AVG(DATEDIFF(start_date_formatted,purchase_date_formatted)) avg_purchase_to_show_days, -- AVG Number of days between purchase and show
				SUM(ticket_counts.ticket_count) / COUNT(o.order_number) as avg_tickets_per_order -- Average Number of Lifetime Tickets Per Order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN seatengine.shows_processed s ON (o.show_id = s.id)
			JOIN (
					SELECT omv.orderNumber, COUNT(omv.unique_id) ticket_count
					FROM seatengine.orders_mv omv
					GROUP BY omv.orderNumber
			) ticket_counts ON (ticket_counts.orderNumber = o.order_number)
			GROUP BY c.subscriber_key
		;
        
        ## TICKET / ORDER STATS FOR LIFETIME PAID ORDERS
        INSERT INTO per_paid_order
			SELECT c.subscriber_key,
				COUNT(o.order_number) total_lifetime_paid_orders, -- Total Lifetime Paid Orders
				SUM(ticket_counts.ticket_count) total_lifetime_paid_tickets, -- Total Number of Lifetime Paid Tickets
				SUM(ticket_counts.ticket_count) /  COUNT(o.order_number) avg_tickets_per_paid_order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN (
					SELECT omv.orderNumber, count(omv.unique_id) ticket_count
					FROM seatengine.orders_mv omv
					WHERE omv.not_comped = 1
					GROUP BY omv.orderNumber
			) ticket_counts ON (ticket_counts.orderNumber = o.order_number)
			GROUP BY c.subscriber_key
		;

		## TICKET / ORDER STATS FOR LIFETIME COMP'D ORDERS
		## COMPUTE LAST COMP'D SHOW DATE PER CUSTOMER
        INSERT INTO per_comp_order
			SELECT c.subscriber_key,
				MAX(start_date_formatted) AS last_comp_show_date, -- Last Comp Show Date
				COUNT(o.order_number) total_lifetime_comp_orders, -- Total Lifetime Comp'd Orders
				SUM(ticket_counts.ticket_count) total_lifetime_comp_tickets, -- Total Lifetime Comp'd Tickets
				SUM(ticket_counts.ticket_count) /  COUNT(o.order_number) avg_tickets_per_comp_order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN seatengine.shows_processed s ON (o.show_id = s.id)
			JOIN (
					SELECT omv.orderNumber, count(omv.unique_id) ticket_count
					FROM seatengine.orders_mv omv
					WHERE omv.comped = 1
					GROUP BY omv.orderNumber
			) ticket_counts ON (ticket_counts.orderNumber = o.order_number)
			GROUP BY c.subscriber_key
        ;
        
        ## EVENT ATTENDANCE COUNTS BY WEEKDAY PER CUSTOMER
        INSERT INTO weekday_attendence
			SELECT wk.cust_id AS subscriber_key,
					COUNT(CASE WHEN show_day = 0 THEN 1 END) AS shows_attended_M, -- Comp'd or Paid Events Attended on Monday
					COUNT(CASE WHEN show_day = 1 THEN 1 END) AS shows_attended_T, -- Comp'd or Paid Events Attended on Tuesday
					COUNT(CASE WHEN show_day = 2 THEN 1 END) AS shows_attended_W, -- Comp'd or Paid Events Attended on Wednesday
					COUNT(CASE WHEN show_day = 3 THEN 1 END) AS shows_attended_R, -- Comp'd or Paid Events Attended on Thursday
					COUNT(CASE WHEN show_day = 4 THEN 1 END) AS shows_attended_F, -- Comp'd or Paid Events Attended on Friday
					COUNT(CASE WHEN show_day = 5 THEN 1 END) AS shows_attended_S, -- Comp'd or Paid Events Attended on Saturday
					COUNT(CASE WHEN show_day = 6 THEN 1 END) AS shows_attended_U -- Comp'd or Paid Events Attended on Sunday
				FROM (
						SELECT o.cust_id AS cust_id,
							# CONVERT START DATE TO A DAY OF THE WEEK!!! 
                            DAYOFWEEK(s.start_date_formatted) AS show_day,
							COUNT(*) AS shows_attended
						FROM seatengine.orders_processed o
						JOIN seatengine.shows_processed s ON (o.show_id = s.id)
						GROUP BY o.cust_id, show_day
					) AS wk
				GROUP BY subscriber_key
		;

		## COUNT ORDERS BY PAYMENT TYPE & PAID ORDER REVENE IN LAST 90,180,360 DAYS PER CUSTOMER
        INSERT INTO last_360
			SELECT
					o.cust_id AS subscriber_key,
					CONVERT((SUM(CASE WHEN o.not_comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 360 DAY AND NOW()
									THEN o.order_total END)/100), DECIMAL) AS paid_orders_revenue_360,
					CONVERT((SUM(CASE WHEN o.not_comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 180 DAY AND NOW()
									THEN o.order_total END)/100), DECIMAL) AS paid_orders_revenue_180,
					CONVERT((SUM(CASE WHEN o.not_comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 90 DAY AND NOW()
									THEN o.order_total END)/100), DECIMAL) AS paid_orders_revenue_90,
					COUNT(CASE WHEN o.not_comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 360 DAY AND NOW()
									THEN 1 END) AS paid_orders_count_360,
					COUNT(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN NOW() - INTERVAL 180 DAY AND NOW()
								THEN 1 END) AS paid_orders_count_180,
					COUNT(CASE WHEN o.not_comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 90 DAY AND NOW()
									THEN 1 END) AS paid_orders_count_90,
					COUNT(CASE WHEN o.comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 360 DAY AND NOW()
									THEN 1 END) AS comp_orders_count_360,
					COUNT(CASE WHEN o.comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 180 DAY AND NOW()
									THEN 1 END) AS comp_orders_count_180,
					COUNT(CASE WHEN o.comped = 1
									AND purchase_date_formatted BETWEEN NOW() - INTERVAL 90 DAY AND NOW()
									THEN 1 END) AS comp_orders_count_90
				FROM seatengine.orders_processed o
				GROUP BY subscriber_key
		;
        
        ## COUNT EVENTS THAT ARE SPECIAL TYPES PER CUSTOMER
        INSERT INTO special_shows
			SELECT
				o.cust_id AS subscriber_key,
                o.phone AS phone,
				MAX(o.purchase_date_formatted) AS last_ordered_date, -- Last Ordered Date
				COUNT(CASE WHEN e.special_event = 1 THEN 1 END) AS count_shows_special, -- Special Event Total Order Count
				COUNT(CASE WHEN e.presents_event = 1 THEN 1 END) AS count_shows_persents -- Presents Event Total Order Count
			FROM seatengine.orders_processed o
			JOIN seatengine.shows_processed s ON (s.id = o.show_id)
			JOIN seatengine.events_processed e ON (e.id = s.event_id)
			GROUP BY subscriber_key
		;
        
        ## TOTAL ORDER SPENDING PER CUSTOMER
        INSERT INTO total_spend
			SELECT
				o.cust_id AS subscriber_key,
                CONVERT(SUM(ol.ticket_price)/100, DECIMAL) AS total_revenue -- Total Revenue from customerid
			FROM seatengine.orders_processed o
			JOIN seatengine.orderlines_processed ol ON (ol.order_number = o.order_number)
			WHERE o.not_comped = 1
			GROUP BY subscriber_key
		;
        
        # turn ON all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 1; 

  SET rc = 0;
END;
$$

DELIMITER ;