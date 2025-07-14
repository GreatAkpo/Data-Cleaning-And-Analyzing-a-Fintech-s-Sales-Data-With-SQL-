/* The complete transaction records are contained in the main_transaction_log table .
To begin our first analysis of the commission paid, we will create a new table with records 
that were sent to the VAS provider being analysed. These records have a column populated by JSON data
 that contains a fixed length string that begins with the word "justbeta" followed by an 11 digit random 
number. The required string starts at the twelfth position.

To create a table with  the required records, a select query to move the required records  will be done first.
*/
CREATE TABLE justbeta_records
SELECT * FROM main_transaction_log
WHERE req LIKE '%justbeta%'
AND req NOT LIKE '%irecharge%';

/* Extracting the required reference can be done in two ways :
Using the substring function : 
**/
update justbeta_records set transaction_ref=SUBSTR(req,13,20);

#Or using the JSON_EXTRACT function present in MySQL

update justbeta_records set transaction_ref=TRIM(REPLACE(JSON_EXTRACT(req,'$.orderId'),'"','')) 

/* From the above, the replace function is used to remove the double quotes that surround a text value 
in JSON. The trim function was used to remove any whitespace at the beginning or end of the string
*/
