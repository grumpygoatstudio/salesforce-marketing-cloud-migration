DROP PROCEDURE refresh_contacts_mv_now;

DELIMITER $$

CREATE PROCEDURE refresh_contacts_mv_now (
    OUT rc INT
)
BEGIN
		# turn OFF all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 0; 
		
		# truncate the existing table 
		TRUNCATE TABLE contacts_mv;
        
        # populate with new data
		INSERT INTO contacts_mv
		SELECT
			c.email_address AS email_address,
			c.name AS cust_name,
			c.name_first AS cust_name_first,
			c.name_last AS cust_name_last,
			phone AS phone,
			total_revenue,  -- Total Revenue from customerid
			last_ordered_date, -- Last Ordered Date
			first_show_attended,  -- First Show Attended Date
			first_event_title, -- First Event Title
            first_event_venue, -- First Event Venue
			last_show_attended, -- Last Show Attended Date
			last_event_title, -- Last Event Title
			last_event_venue, -- Last Event Venue
            next_show_attending, -- Next Show Attending Date
			next_event_title, -- Next Event Title
            next_event_venue, -- Next Event Venue
			avg_tickets_per_order, -- Average Number of Lifetime Tickets Per Order
			avg_purchase_to_show_days, -- AVG Number of days between purchase and show
			total_lifetime_paid_orders + total_lifetime_comp_orders AS total_orders, -- Total Orders
            total_lifetime_paid_orders, -- Total Lifetime Paid Orders
			total_lifetime_paid_tickets, -- Total Number of Lifetime Paid Tickets
			avg_tickets_per_paid_order, -- Average Number of Lifetime Tickets Per Paid Order
			total_lifetime_comp_orders, -- Total Lifetime Comp'd Orders
			total_lifetime_comp_tickets, -- Total Number of Lifetime Comp'd Tickets
			avg_tickets_per_comp_order, -- Average Number of Lifetime Tickets Per Comp'd Order
			last_comp_show_date, -- Last Comp Show Date
			shows_attended_M, -- Comp'd or Paid Events Attended on Monday
			shows_attended_T, -- Comp'd or Paid Events Attended on Tuesday
			shows_attended_W, -- Comp'd or Paid Events Attended on Wednesday
			shows_attended_R, -- Comp'd or Paid Events Attended on Thursday
			shows_attended_F, -- Comp'd or Paid Events Attended on Friday
			shows_attended_S, -- Comp'd or Paid Events Attended on Saturday
			shows_attended_U, -- Comp'd or Paid Events Attended on Sunday
			paid_orders_revenue_360, -- Total Paid Ticket Revenue Over 90 Days
			paid_orders_count_360, -- Total Paid Orders Over Last 90 days
			comp_orders_count_360, -- Total Comp'd Orders Over Last 90 days
			paid_orders_revenue_180, -- Total Paid Ticket Revenue Over 180 Days
			paid_orders_count_180, -- Total Paid Orders Over Last 180 days
			comp_orders_count_180, -- Total Comp'd Orders Over Last 180 days
			paid_orders_revenue_90, -- Total Paid Ticket Revenue Over 365 Days
			paid_orders_count_90, -- Total Paid Orders Over Last 365 days
			comp_orders_count_90, -- Total Comp'd Orders Over Last 365 days
			count_shows_special, -- Special Event Total Order Count
			count_shows_persents, -- Presents shows Total Order Count
            c.sys_entry_date AS sys_entry_date
		FROM seatengine.contacts c
		LEFT JOIN fln_event_data ON (c.email_address = fln_event_data.subscriber_key) 
		LEFT JOIN avg_tickets_per_order ON (c.email_address = avg_tickets_per_order.subscriber_key)
		LEFT JOIN per_paid_order ON (c.email_address = per_paid_order.subscriber_key)
		LEFT JOIN per_comp_order ON (c.email_address = per_comp_order.subscriber_key)
		LEFT JOIN weekday_attendence ON (c.email_address = weekday_attendence.subscriber_key)
		LEFT JOIN last_360 ON (c.email_address = last_360.subscriber_key)
		LEFT JOIN total_spend ON (c.email_address = total_spend.subscriber_key)
        LEFT JOIN special_shows ON (c.email_address = special_shows.subscriber_key)
        GROUP BY c.email_address; 
        
        # turn ON all FK constraints for tables 
        SET FOREIGN_KEY_CHECKS = 1; 
        
  SET rc = 0;
END;
$$

DELIMITER ;
