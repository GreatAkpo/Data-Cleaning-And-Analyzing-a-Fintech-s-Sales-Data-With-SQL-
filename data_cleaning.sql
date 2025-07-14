/* Two tables contain the required data :
a) xml_val_pre_2023
b) xml_val

There is an overlap of records in both tables, so there is a need to join them together into 
a single table to perform the analysis.

To perform the aforementioned, a UNION query will be executed.
*/

CREATE TABLE consolidated_records
SELECT * FROM xml_val_pre_2023
UNION 
SELECT * FROM xml_val ORDER BY trans_date;