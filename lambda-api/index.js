var mysql = require('mysql');
var pool = mysql.createPool({
  host     : process.env.MYSQL_HOST,
  user     : process.env.MYSQL_USER,
  password : process.env.MYSQL_PASSWORD,
  database : process.env.MYSQL_DB
});

exports.handler = (event, context, callback) => {
    // console.log('Received event:', JSON.stringify(event, null, 2));
    //prevent timeout from waiting event loop
    context.callbackWaitsForEmptyEventLoop = false;
    var url = event.path;
    // check for webhook endpoints vs. mobile upload (ie. bare '/')
    // doubly check for correct HTTP Request Method
    if (url.indexOf('/webhooks') > -1 && event.httpMethod == 'POST') {
        // parser the stringy x-form POST event data
        var results = {};
        decodeURIComponent(event.body).split('&').forEach(function(i) { 
            var key_val = i.split('=');
            if (key_val[0].indexOf('contact[email]') > -1) {
                results['email'] = key_val[1];
            } else if (key_val[0].indexOf('contact[fields][mobile_number]') > -1) {
                results['mobile'] = key_val[1];
            } else if (key_val[0].indexOf('contact[fields][mobile_opt') > -1) {
                results['optin'] = key_val[1];
            }
        });
        
        // determine which trigger was sent and handle accordingly
        if (url.indexOf('new-contact') > -1) {
            console.log('Received NEW CONTACT trigger:', JSON.stringify(results, null, 2));
            // Build SQL query & INSERT INTO ac_mobile_contacts
            // Last Updated & Created Dates default to DATETIME.NOW()
            // now = datetime.now().toString()
            // INSERT INTO ac_mobile_contacts (email, mobile, optin)
            // VALUES( '<email-address>', '<mobile-number>', '<optin-status>')
            // ON DUPLICATE KEY UPDATE
            // mobile = '<mobile-number>', email = '<email-address>', optin = '<optin-status>',
            // last_updated = datetime.now()
            
            // Send callback response to API
            callback(null, {
                "isBase64Encoded": false,
                "statusCode": 200,
                "headers": { "Content-Type": 'application/json' },
                "body": JSON.stringify("Received NEW CONTACT trigger"),
            });
        } else if (url.indexOf('mobile-chng') > -1) {
            console.log('Received MOBILE CHANGE trigger:', JSON.stringify(results, null, 2));
            // Build SQL query & UPDATE ac_mobile_contacts
            // UPDATE ac_mobile_contacts 
            // SET optin = "<optin-status>", mobile = "<mobile-number>"
            // WHERE email = "<email-address>";
            
            // Send callback response to API
            callback(null, {
                "isBase64Encoded": false,
                "statusCode": 200,
                "headers": { "Content-Type": 'application/json' },
                "body": JSON.stringify("Received MOBILE CHANGE trigger"),
            });
        } else {
            callback({
                "isBase64Encoded": false,
                "statusCode": 500,
                "headers": { "Content-Type": 'application/json' },
                "body": {'error': "Unsupported method."},
            });
        }
    // check that the root path has been requested
    // doubly check for correct HTTP Request Method
    } else if (url == "/" && event.httpMethod == 'GET') {
        pool.getConnection(function(err, conn) {
            if (err) {
                callback(err);
            } else {
                // Build the SQL query
                var sql = 'INSERT INTO mobile_uploads (mobile_number, keyword, security_code, campaign, body, email, timestamp) VALUES ('
                    + conn.escape(event.queryStringParameters.m) + ', ' // keyword
                    + conn.escape(event.queryStringParameters.k) + ', ' // mobile number
                    + conn.escape(event.queryStringParameters.s) + ', ' // security code
                    + conn.escape(event.queryStringParameters.c) + ', ' // campaign code
                    + conn.escape(event.queryStringParameters.b) + ', ' // body text
                    + conn.escape(event.queryStringParameters.e) + ', ' // email
                    + conn.escape(event.queryStringParameters.t) // timestamp
                    + ');';
            
                // Use the connection
                conn.query(sql, function (error, results, fields) {
                    // Done with the connection.
                    conn.release();
                    // Handle error after the release.
                    if (error) {
                        console.log('Connection error occured.', error.stack);
                        callback({
                            "isBase64Encoded": false,
                            "statusCode": 500,
                            "headers": { "Content-Type": 'application/json' },
                            "body": JSON.stringify(error),
                        });
                    } else {
                        callback(null, {
                            "isBase64Encoded": false,
                            "statusCode": 200,
                            "headers": { "Content-Type": 'application/json' },
                            "body": JSON.stringify(results),
                            
                        });
                    }
                });
            }
        });
    } else {
        callback({
            "isBase64Encoded": false,
            "statusCode": 500,
            "headers": { "Content-Type": 'application/json' },
            "body": {'error': "Unsupported method or url."},
        });
    }
};
