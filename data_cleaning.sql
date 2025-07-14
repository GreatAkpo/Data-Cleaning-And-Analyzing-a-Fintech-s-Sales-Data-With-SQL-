/* The complete transaction records are contained in the main_transaction_log table .
To begin our first analysis of the commission paid, we will create a new table with records 
that were sent to the VAS provider being analysed. These records have a column that contains 
a fixed length string that begins with the word "justbeta" followed by an 11 digit random 
number. The required string starts at the twelfth position.


To perform the aforementioned, a select query to move the required records to a new table will be done first.
*/
CREATE TABLE justbeta_records
SELECT * FROM main_transaction_log
WHERE req LIKE '%justbeta%'
AND req NOT LIKE '%irecharge%';