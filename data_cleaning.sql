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

/*The first task is to determine if the transaction_refs in the justbeta_records, match the refs 
in the providers table. A table was created to hold these records with their corresponding matched value
The query to achieve this is below :
*/


CREATE TABLE matched_status SELECT justbeta_records.transaction_type,transaction_ref local_ref,ref provider_ref,
 transaction_status,justbeta_records.amount,CASE WHEN justbeta_records.transaction_ref=provider_records.ref
THEN 'Matched' ELSE 'Not Matched' END AS Matched_Status,resp server_response FROM 
justbeta_records
LEFT OUTER JOIN provider_records ON (transaction_ref=ref)

/* We now extracted the several types of transactions carried out **/

SELECT transaction_type
FROM matched_status
GROUP BY transaction_type

/* A table to hold the commission percentages for each transaction type was now created.
This will enable us generate the expected commission, and match it against the value paid out by the 
VAS provider */


CREATE TABLE `commission_percentages` (
  `id` int NOT NULL AUTO_INCREMENT,
  `transaction_type` varchar(50) DEFAULT NULL,
  `commission_percentage` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

/* Insert commission values */
insert  into `commission_percentages`(`id`,`transaction_type`,`commission_percentage`) values 
(1,'JOS ELECTRICITY DISTRIBUTION','2'),
(2,'JOS ELECTRICITY POSTPAID','2'),
(3,'AIRTEL VTU','3.5'),
(4,'MTN VTU','3.3'),
(5,'GLO VTU','5.2'),
(6,'AIRTEL DATA','3.5'),
(7,'GLO DATA','5.2'),
(8,'KANO ELECTRICITY DISCO','1.7'),
(9,'ETISALAT VTU','6'),
(10,'AEDC PREPAID','1.7'),
(11,'(NULL)IKEJA TOKEN PURCHASE','1'),
(12,'BENIN ELECTRICITY DISTRIBUTION COMPANY PREPAID','1.7'),
(13,'ETISALAT DATA','6'),
(14,'AEDC POSTPAID','1.7'),
(15,'IBADAN DISCO PREPAID','1'),
(16,'BENIN ELECTRICITY DISTRIBUTION COMPANY POSTPAID','1.7');



DELIMITER $$

CREATE
   
    FUNCTION `provider_records`.`get_commission`(transaction_type_val VARCHAR(50))
    RETURNS VARCHAR(5)
    /*LANGUAGE SQL
    | [NOT] DETERMINISTIC
    | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
    | SQL SECURITY { DEFINER | INVOKER }
    | COMMENT 'string'*/
    BEGIN
    DECLARE commission_percentage_val VARCHAR(5);
    SELECT commission_percentage INTO commission_percentage_val FROM 
    commission_percentages WHERE transaction_type=transaction_type_val;
    RETURN commission_percentage_val;
    END$$

DELIMITER ;




