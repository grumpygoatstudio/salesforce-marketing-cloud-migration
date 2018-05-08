DROP TABLE contacts_mv;
CREATE TABLE contacts_mv (
    subscriber_key  VARCHAR(255)    NOT NULL,
    cust_name   VARCHAR(255)    NOT NULL,
    email_address   VARCHAR(255)    NULL,
    phone   VARCHAR(255)    NULL,
    total_revenue   VARCHAR(255)    NULL, -- Total Revenue from Customer
    last_ordered_date   DATE    NULL, -- Last Ordered Date
    first_show_attended DATE    NULL,  -- First Show Attended Date
    first_event_title VARCHAR(255)    NULL, -- First Event Title
    first_event_venue VARCHAR(255)    NULL, -- First Event Venue
    last_show_attended DATE    NULL, -- Last Show Attended Date
    last_event_title VARCHAR(255)    NULL, -- Last Event Title
    last_event_venue VARCHAR(255)    NULL, -- Last Event Venue
    next_show_attending DATE    NULL, -- Next Show Attending Date
    next_event_title VARCHAR(255)    NULL, -- Next Event Title
    avg_tickets_per_order FLOAT    NULL, -- Average Number of Lifetime Tickets Per Order
    avg_purchase_to_show_days INTEGER(25)    NULL, -- AVG Number of days between purchase and show
    total_lifetime_paid_orders INTEGER(25)    NULL, -- Total Lifetime Paid Orders
    total_lifetime_paid_tickets INTEGER(25)    NULL, -- Total Number of Lifetime Paid Tickets
    avg_tickets_per_paid_order FLOAT    NULL, -- Average Number of Lifetime Tickets Per Paid Order
    total_lifetime_comp_orders INTEGER(25)    NULL, -- Total Lifetime Comp'd Orders
    total_lifetime_comp_tickets INTEGER(25)    NULL, -- Total Number of Lifetime Comp'd Tickets
    avg_tickets_per_comp_order FLOAT(25)    NULL, -- Average Number of Lifetime Tickets Per Comp'd Order
    last_comp_show_date DATE    NULL, -- Last Comp Show Date
    shows_attended_M INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Monday
    shows_attended_T INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Tuesday
    shows_attended_W INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Wednesday
    shows_attended_R INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Thursday
    shows_attended_F INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Friday
    shows_attended_S INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Saturday
    shows_attended_U INTEGER(25)    NULL, -- Comp'd or Paid Events Attended on Sunday
    paid_orders_revenue_360 INTEGER(25)    NULL, -- Total Paid Ticket Revenue Over 90 Days
    paid_orders_count_360 INTEGER(25)    NULL, -- Total Paid Orders Over Last 90 days
    comp_orders_count_360 INTEGER(25)    NULL, -- Total Comp'd Orders Over Last 90 days
    paid_orders_revenue_180 INTEGER(25)    NULL, -- Total Paid Ticket Revenue Over 180 Days
    paid_orders_count_180 INTEGER(25)    NULL, -- Total Paid Orders Over Last 180 days
    comp_orders_count_180 INTEGER(25)    NULL, -- Total Comp'd Orders Over Last 180 days
    paid_orders_revenue_90 INTEGER(25)    NULL, -- Total Paid Ticket Revenue Over 365 Days
    paid_orders_count_90 INTEGER(25)    NULL, -- Total Paid Orders Over Last 365 days
    comp_orders_count_90 INTEGER(25)    NULL, -- Total Comp'd Orders Over Last 365 days
    count_shows_special INTEGER(25)    NULL , -- Special Event Total Order Count
    count_shows_persents INTEGER(25)    NULL -- Presents shows Total Order Count
);
