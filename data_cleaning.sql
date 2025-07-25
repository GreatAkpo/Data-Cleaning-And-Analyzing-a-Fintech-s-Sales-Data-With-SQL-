/* The complete transaction records are contained in the main_transaction_log table .
To begin our
 analysis, we will create a new table with records 
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

/*This table now fulfills the first objective of matching references.
 We now extracted the several types of transactions carried out **/

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

/* A function was now created to fetch the commission percentage 
that was used for the basis of our calculation */
DELIMITER $$


DROP FUNCTION IF EXISTS `get_commission_percentage`$$

CREATE FUNCTION `get_commission_percentage`(transaction_type_val VARCHAR(50)) RETURNS VARCHAR(5) CHARSET utf8mb4
    READS SQL DATA
    COMMENT 'Fetches the commission percentage from the commission_percentage table, for the given transaction type'
BEGIN
    DECLARE commission_percentage_val VARCHAR(50);
    SELECT commission_percentage INTO commission_percentage_val FROM 
    commission_percentages WHERE transaction_type=transaction_type_val;
    RETURN commission_percentage_val;
    END$$

DELIMITER ;

/* Another function was created to fetch the amount
that was used to generate the commission on the providers table.
This amount will be used to compare with the amount on the justbeta_records table */

DELIMITER $$


DROP FUNCTION IF EXISTS `get_transaction_amount`$$

CREATE  FUNCTION `get_transaction_amount`(ref_val VARCHAR(50)) RETURNS VARCHAR(50) CHARSET utf8mb4
    READS SQL DATA
    COMMENT 'Fetches the transaction amount from the providers table, for the given ref'
BEGIN
    DECLARE transaction_val VARCHAR(50);
    SELECT amount INTO transaction_val FROM 
    provider_records WHERE ref=ref_val
    AND operation_type='vend';
    RETURN transaction_val;
    END$$

DELIMITER ;




/* Another function was created to fetch the commission on the providers table.
This amount will be used to compare with the calculated commission based on the
transaction amount in the justbeta_records table */

DELIMITER $$


DROP FUNCTION IF EXISTS `get_commission`$$

CREATE FUNCTION `get_commission`(ref_val VARCHAR(50)) RETURNS VARCHAR(5) CHARSET utf8mb4
    READS SQL DATA
    COMMENT 'Fetches the commission from the providers table, for the given ref'
BEGIN
    DECLARE commission_val VARCHAR(50);
    SELECT amount INTO commission_val FROM 
    provider_records WHERE ref=ref_val
    AND operation_type='commission';
    RETURN commission_val;
    END$$

DELIMITER ;

/* Three columns were now created on the matched_status table in order 
to hold the return values of the 3 newly created functions **/

ALTER TABLE `matched_status` 
ADD COLUMN `provider_amount` VARCHAR(45) NULL AFTER `server_response`,
ADD COLUMN `provider_commission` VARCHAR(45) NULL AFTER `provider_amount`,
ADD COLUMN `commission_percentage` VARCHAR(45) NULL AFTER `provider_commission`;


/* The provider_amount is populated using the below query*/

UPDATE matched_status
SET provider_amount=get_transaction_amount(local_ref);

/* To get the commission paid by the provider, the get_commission function was called
However this error was thrown : 'Error Code: 1172
Result consisted of more than one row'
This means that duplicate records exist.
To find these records, a new table was created using the following query 
utilizing a window function
 */
CREATE TABLE provider_row_numbers
SELECT amount,ref,operation_type,ROW_NUMBER() OVER(
PARTITION BY ref ORDER BY ref
) row_num
 FROM provider_records WHERE operation_type="commission"
 ORDER BY ref

 /** To eliminate these records with row numbers greater than 1,
 a new table was created with single row numbers , ie row  numbers equal to 1 **/

CREATE TABLE provider_single_row_numbers
SELECT * FROM provider_row_numbers
WHERE row_num=1;

/** The get_commission function was now altered to fetch records from the newly created
table **/

DELIMITER $$


DROP FUNCTION IF EXISTS `get_commission`$$

CREATE FUNCTION `get_commission`(ref_val VARCHAR(50)) RETURNS VARCHAR(50) CHARSET utf8mb4
    READS SQL DATA
    COMMENT 'Fetches the commission from the provider_single_row_numbers table, for the given ref'
BEGIN
    DECLARE commission_val VARCHAR(50);
    SELECT amount INTO commission_val FROM 
    provider_single_row_numbers WHERE ref=ref_val
    AND operation_type='commission';
    RETURN commission_val;
    END$$

DELIMITER ;

/** The matched table records will now be spooled into a new table that 
will contain row numbers, which will be used to eliminate duplicate records **/

CREATE TABLE matched_status_single_row SELECT *,ROW_NUMBER() OVER (
PARTITION BY local_ref ORDER BY local_ref
) row_num
FROM matched_status;

/** Row numbers grater than 1 will now be removed **/

DELETE from matched_status_single_row
where row_num > 1;

/** The remaining analysis for discovering commission discrepancies , is done in a Jupyter notebook,
after the data from the matched_status_single_row table was exported.
The notebook is included in the repository
**/ 

/*The final stage of analysis is to discover if the correct amount of funds paid, were used 
to fund the VAS provider wallet. A table was created holding the funding records
on the local server **/
CREATE TABLE agent_funding_requests 
SELECT YEAR(trans_date) funding_year,DATE_FORMAT(trans_date,'%M') funding_month,
SUM(amount) total_funding
FROM main_transaction_log
WHERE transaction_type='Virtual Account Funding'
GROUP BY YEAR(trans_date),DATE_FORMAT(trans_date,'%M') ;

/* A second table holding the records of wallet funding operations done by the VAS 
provider was also created **/
CREATE TABLE provider_funding 
SELECT YEAR(transaction_time) funding_year,DATE_FORMAT(transaction_time,'%M') funding_month,
SUM(amount) total_funding
FROM provider_records
WHERE operation_type='transfer_topup'
GROUP BY YEAR(transaction_time),DATE_FORMAT(transaction_time,'%M') ;




/**
A CTE query was executed to find out the different fundings
done by month and year, by the agents and on the VAS portal. The difference in these amounts
was now calculated **/
WITH agent_funding AS
(
SELECT funding_year,
funding_month,
total_funding
FROM agent_funding_requests
)
SELECT agent_funding.funding_year agent_funding_year,agent_funding.funding_month agent_funding_month,
COALESCE(provider_funding.funding_year,"No Provider Year Value") provider_funding_year,
COALESCE(provider_funding.funding_month,"No Provider Month Value")  provider_funding_month,
FORMAT(agent_funding.total_funding,2) agent_total_funding,
FORMAT(COALESCE(provider_funding.total_funding,0),2) provider_total_funding,FORMAT(agent_funding.total_funding-(COALESCE(provider_funding.total_funding,0)),2)
funding_difference
FROM agent_funding
LEFT JOIN provider_funding
ON (agent_funding.funding_year=provider_funding.funding_year)
AND (agent_funding.funding_month=provider_funding.funding_month)










