DROP PROCEDURE refresh_contacts_mobile_mv;

DELIMITER $$

CREATE PROCEDURE refresh_contacts_mobile_mv (
    OUT rc INT
)
BEGIN
    # turn OFF all FK constraints for tables 
    SET FOREIGN_KEY_CHECKS = 0; 

    # truncate the existing table 
    TRUNCATE TABLE contacts_mobile_mv;

    # update ac_mobile_contacts & contacts_mv to ensure mobile numbers are clean
    UPDATE ac_mobile_contacts SET mobile_number = ONLY_NUMBERS(mobile_number);
    UPDATE contacts_mv SET phone = ONLY_NUMBERS(phone) WHERE phone is not null AND phone != "";

    # check for mobile matches in SE & AC and update email addresses when found
    UPDATE seatengine.mobile_uploads mu, seatengine.ac_mobile_contacts ac
        SET mu.email = ac.email
        WHERE mu.mobile_number = ac.mobile_number
        AND mu.email IS NULL;

    UPDATE seatengine.mobile_uploads mu, seatengine.contacts_mv se
        SET mu.email = se.email_address
        WHERE mu.mobile_number = ONLY_NUMBERS(se.phone)
        AND mu.email IS NULL;

    # populate contacts_mobile_mv with newly updated data
    INSERT INTO contacts_mobile_mv
    SELECT 
        mu.mobile_number as mobile_number, 
        CASE 
            WHEN mu.body LIKE '%STOP%' THEN 'Unsubscribed'
            ELSE 'Subscribed'
        END as mobile_status,
        mu.email as email_address,
        mu_min.join_date as join_date,
        MAX(DATE_FORMAT(mu.timestamp, '%Y-%m-%d %H:%i:%S')) as last_message_date
    FROM seatengine.mobile_uploads mu
    JOIN (
      SELECT mobile_number, MIN(DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:%S')) as join_date
      FROM seatengine.mobile_uploads
      GROUP BY mobile_number
    ) AS mu_min ON mu.mobile_number =  mu_min.mobile_number
    GROUP BY mu.mobile_number;

    # turn ON all FK constraints for tables 
    SET FOREIGN_KEY_CHECKS = 1; 
        
    SET rc = 0;
END;
$$

DELIMITER ;
