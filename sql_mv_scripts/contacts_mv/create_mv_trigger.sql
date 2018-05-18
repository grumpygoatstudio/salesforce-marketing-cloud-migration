DROP PROCEDURE refresh_contacts_mv_now;

DELIMITER $$

CREATE PROCEDURE refresh_contacts_mv_now (
    OUT rc INT
)
BEGIN
		TRUNCATE TABLE contacts_mv;
		INSERT INTO contacts_mv
		SELECT
			c.subscriber_key AS subscriber_key,
			c.name AS cust_name,
			c.email_address AS email_address,
			o.phone AS phone,
			sum(o.order_total) total_revenue, -- Total Revenue from Customer
			max(o.purchase_date_formatted) last_ordered_date, -- Last Ordered Date
			first_attended.first_show_attended,  -- First Show Attended Date
			first_attended.first_event_title, -- First Event Title
            first_attended.first_event_venue, -- First Event Venue
			last_attended.last_show_attended, -- Last Show Attended Date
			last_attended.last_event_title, -- Last Event Title
			last_attended.last_event_venue, -- Last Event Venue
            next_attending.next_show_attending, -- Next Show Attending Date
			next_attending.next_event_title, -- Next Event Title
			avg_tickets_per_order.avg_tickets_per_order, -- Average Number of Lifetime Tickets Per Order
			avg_tickets_per_order.avg_purchase_to_show_days, -- AVG Number of days between purchase and show
			per_paid_order.total_lifetime_paid_orders, -- Total Lifetime Paid Orders
			per_paid_order.total_lifetime_paid_tickets, -- Total Number of Lifetime Paid Tickets
			per_paid_order.avg_tickets_per_paid_order, -- Average Number of Lifetime Tickets Per Paid Order
			per_comp_order.total_lifetime_comp_orders, -- Total Lifetime Comp'd Orders
			per_comp_order.total_lifetime_comp_tickets, -- Total Number of Lifetime Comp'd Tickets
			per_comp_order.avg_tickets_per_comp_order, -- Average Number of Lifetime Tickets Per Comp'd Order
			per_comp_order.last_comp_show_date, -- Last Comp Show Date
			shows_attended_M, -- Comp'd or Paid Events Attended on Monday
			shows_attended_T, -- Comp'd or Paid Events Attended on Tuesday
			shows_attended_W, -- Comp'd or Paid Events Attended on Wednesday
			shows_attended_R, -- Comp'd or Paid Events Attended on Thursday
			shows_attended_F, -- Comp'd or Paid Events Attended on Friday
			shows_attended_S, -- Comp'd or Paid Events Attended on Saturday
			shows_attended_U, -- Comp'd or Paid Events Attended on Sunday
			last_360.paid_orders_revenue_360, -- Total Paid Ticket Revenue Over 90 Days
			last_360.paid_orders_count_360, -- Total Paid Orders Over Last 90 days
			last_360.comp_orders_count_360, -- Total Comp'd Orders Over Last 90 days
			last_360.paid_orders_revenue_180, -- Total Paid Ticket Revenue Over 180 Days
			last_360.paid_orders_count_180, -- Total Paid Orders Over Last 180 days
			last_360.comp_orders_count_180, -- Total Comp'd Orders Over Last 180 days
			last_360.paid_orders_revenue_90, -- Total Paid Ticket Revenue Over 365 Days
			last_360.paid_orders_count_90, -- Total Paid Orders Over Last 365 days
			last_360.comp_orders_count_90, -- Total Comp'd Orders Over Last 365 days
			special_shows.count_shows_special, -- Special Event Total Order Count
			special_shows.count_shows_persents -- Presents shows Total Order Count
		FROM seatengine.contacts c
		LEFT JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
        LEFT JOIN seatengine.shows_processed s ON (o.show_id = s.id)
        LEFT JOIN seatengine.events_processed e ON (s.event_id = e.id)
        LEFT JOIN seatengine.venues_processed v ON (e.venue_id = v.id)

		## FIRST SHOW / EVENT INFO PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				MIN(start_date_formatted) AS first_show_attended, -- First Show Attended Date
				e.name AS first_event_title, -- First Event Title
                v.id AS first_event_venue -- First Event Venue
			FROM seatengine.contacts c
				JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
				JOIN seatengine.shows_processed s ON (s.id = o.show_id)
				JOIN seatengine.events_processed e ON (e.id = s.event_id)
                JOIN seatengine.venues_processed v ON (v.id = e.venue_id)
			WHERE start_date_formatted <= CURDATE()
			GROUP BY c.subscriber_key
		) as first_attended ON (first_attended.subscriber_key = c.subscriber_key)

		## LAST SHOW / EVENT INFO PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				MAX(start_date_formatted) AS last_show_attended, -- Last Show Attended Date
				e.name AS last_event_title, -- Last Event Title
                v.id AS last_event_venue -- Last Event Venue
			FROM seatengine.contacts c
				JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
				JOIN seatengine.shows_processed s ON (s.id = o.show_id)
				JOIN seatengine.events_processed e ON (e.id = s.event_id)
                JOIN seatengine.venues_processed v ON (v.id = e.venue_id)
			WHERE start_date_formatted <= CURDATE()
			GROUP BY c.subscriber_key
		) as last_attended ON (last_attended.subscriber_key = c.subscriber_key)

		## NEXT SHOW / EVENT INFO PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				MIN(start_date_formatted) AS next_show_attending, -- Next Show Attending Date
				e.name AS next_event_title -- Next Event Title
			FROM seatengine.contacts c
				JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
				JOIN seatengine.shows_processed s ON (s.id = o.show_id)
				JOIN seatengine.events_processed e ON (e.id = s.event_id)
			WHERE start_date_formatted > CURDATE()
			GROUP BY c.subscriber_key
		) as next_attending ON (next_attending.subscriber_key = c.subscriber_key)

		## AVERAGE TICKETS PURCHASED PER CUSTOMER LIMETIME ORDERS
		## COMPUTE AVG NUMBER OF DAYS BETWEEN PURCHASE DATE AND SHOW PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				-- AVG Number of days between purchase and show
				AVG(start_date_formatted - purchase_date_formatted) avg_purchase_to_show_days,
				-- Average Number of Lifetime Tickets Per Order
				SUM(ticket_counts.ticket_count) /  COUNT(o.order_number) as avg_tickets_per_order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN seatengine.shows_processed s ON (o.show_id = s.id)
			JOIN (
					SELECT o.order_number, count(ol.id) ticket_count
					FROM seatengine.orders_processed o
					JOIN seatengine.orderlines_processed ol ON (ol.order_number = o.order_number)
					GROUP BY o.order_number
			) ticket_counts ON (ticket_counts.order_number = o.order_number)
			GROUP BY c.subscriber_key
		) as avg_tickets_per_order ON (avg_tickets_per_order.subscriber_key = c.subscriber_key)

		## TICKET / ORDER STATS FOR LIFETIME PAID ORDERS
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				COUNT(o.order_number) total_lifetime_paid_orders, -- Total Lifetime Paid Orders
				SUM(ticket_counts.ticket_count) total_lifetime_paid_tickets, -- Total Number of Lifetime Paid Tickets
				SUM(ticket_counts.ticket_count) /  COUNT(o.order_number) avg_tickets_per_paid_order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN (
					SELECT o.order_number, count(ol.id) ticket_count
					FROM seatengine.orders_processed o
					JOIN seatengine.orderlines_processed ol ON (ol.order_number = o.order_number)
					WHERE o.not_comped = 1
					GROUP BY o.order_number
			) ticket_counts ON (ticket_counts.order_number = o.order_number)
			GROUP BY c.subscriber_key
		) as per_paid_order ON (per_paid_order.subscriber_key = c.subscriber_key)

		## TICKET / ORDER STATS FOR LIFETIME COMP'D ORDERS
		## COMPUTE LAST COMP'D SHOW DATE PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT c.subscriber_key,
				MAX(start_date_formatted) AS last_comp_show_date, -- Last Comp Show Date
				COUNT(o.order_number) total_lifetime_comp_orders, -- Total Lifetime Comp'd Orders
				SUM(ticket_counts.ticket_count) total_lifetime_comp_tickets, -- Total Lifetime Comp'd Tickets
				SUM(ticket_counts.ticket_count) /  COUNT(o.order_number) avg_tickets_per_comp_order
			FROM seatengine.contacts c
			JOIN seatengine.orders_processed o ON (o.cust_id = c.subscriber_key)
			JOIN seatengine.shows_processed s ON (o.show_id = s.id)
			JOIN (
					SELECT o.order_number, count(ol.id) ticket_count
					FROM seatengine.orders_processed o
					JOIN seatengine.orderlines_processed ol ON (ol.order_number = o.order_number)
					WHERE o.comped = 1
					GROUP BY o.order_number
			) ticket_counts ON (ticket_counts.order_number = o.order_number)
			GROUP BY c.subscriber_key
		) as per_comp_order ON (per_comp_order.subscriber_key = c.subscriber_key)

		## EVENT ATTENDANCE COUNTS BY WEEKDAY PER CUSTOMER
		LEFT OUTER JOIN (
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
						s.start_date_formatted AS show_day,
						COUNT(*) AS shows_attended
					FROM seatengine.orders_processed o
					JOIN seatengine.shows_processed s ON (o.show_id = s.id)
					GROUP BY o.cust_id, show_day
				) AS wk
			GROUP BY subscriber_key
		) as weekday_attendence ON (weekday_attendence.subscriber_key = c.subscriber_key)

		## COUNT ORDERS BY PAYMENT TYPE & PAID ORDER REVENE IN LAST 90,180,360 DAYS PER CUSTOMER
		LEFT OUTER JOIN (
				SELECT
				o.cust_id AS subscriber_key,
				SUM(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 360 DAY AND CURDATE()
								THEN o.order_total END) AS paid_orders_revenue_360,
				SUM(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 180 DAY AND CURDATE()
								THEN o.order_total END) AS paid_orders_revenue_180,
				SUM(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 90 DAY AND CURDATE()
								THEN o.order_total END) AS paid_orders_revenue_90,
				COUNT(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 360 DAY AND CURDATE()
								THEN 1 END) AS paid_orders_count_360,
				COUNT(CASE WHEN o.not_comped = 1
							AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 180 DAY AND CURDATE()
							THEN 1 END) AS paid_orders_count_180,
				COUNT(CASE WHEN o.not_comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 90 DAY AND CURDATE()
								THEN 1 END) AS paid_orders_count_90,
				COUNT(CASE WHEN o.comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 360 DAY AND CURDATE()
								THEN 1 END) AS comp_orders_count_360,
				COUNT(CASE WHEN o.comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 180 DAY AND CURDATE()
								THEN 1 END) AS comp_orders_count_180,
				COUNT(CASE WHEN o.comped = 1
								AND purchase_date_formatted BETWEEN CURDATE() - INTERVAL 90 DAY AND CURDATE()
								THEN 1 END) AS comp_orders_count_90
			FROM seatengine.orders_processed o
			GROUP BY subscriber_key
		) as last_360 ON (last_360.subscriber_key = c.subscriber_key)

		## COUNT EVENTS THAT ARE SPECIAL TYPES PER CUSTOMER
		LEFT OUTER JOIN (
			SELECT
				o.cust_id AS subscriber_key,
				SUM(e.special_event) AS count_shows_special, -- Special Event Total Order Count
				SUM(e.presents_event) AS count_shows_persents -- Presents Event Total Order Count
			FROM seatengine.orders_processed o
			JOIN seatengine.shows_processed s ON (s.id = o.show_id)
			JOIN seatengine.events_processed e ON (e.id = s.event_id)
			GROUP BY subscriber_key
		) as special_shows ON (special_shows.subscriber_key = c.subscriber_key)

		GROUP BY c.subscriber_key;
  SET rc = 0;
END;
$$

DELIMITER ;
