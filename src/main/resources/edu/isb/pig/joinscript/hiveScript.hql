
-- open console and navigate to path - cd /home/ubuntu/work/apache-hive-1.0.0/bin
-- Start hive script  - ./hive
-- Now execute the following instructions for creating the database and the tables 

--enables the cli to show the current database
set hive.cli.print.current.db=true;

--create the database
CREATE DATABASE IF NOT EXISTS taxmanagement LOCATION '/user/ubuntu/hive/warehouse';

--Verify the database created
DESCRIBE DATABASE taxmanagement;
DESCRIBE DATABASE EXTENDED taxmanagement;

--Change to the new database to create the tables
USE taxmanagement;

--Find the available tables in the database. This list should be blank as the database is created newly.
SHOW TABLES;

--create the tax table
CREATE TABLE IF NOT EXISTS taxmanagement.tax (
  pid STRING,
  legalType STRING,
  folio STRING,
  landCoordinate STRING,
  zoneName STRING,
  zoneCategory STRING,
  plan STRING,
  currentLandValue DOUBLE,
  taxAssessmentYear INT,
  taxPaid DOUBLE
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/ubuntu/hive/warehouse/taxmanagement.db/tax';

--verify the table created
SHOW TABLES;
DESCRIBE FORMATTED  taxmanagement.tax;

--create the address table
CREATE TABLE IF NOT EXISTS taxmanagement.address (
  folio STRING,
  streetName STRING, 
  postalCode STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/ubuntu/hive/warehouse/taxmanagement.db/address';


--verify the table created
SHOW TABLES;
DESCRIBE FORMATTED  taxmanagement.address;


--load data into the tables
LOAD DATA LOCAL INPATH '/home/ubuntu/Downloads/data/201*.csv' OVERWRITE INTO TABLE tax;
LOAD DATA LOCAL INPATH '/home/ubuntu/Downloads/data/Address.csv' OVERWRITE INTO TABLE address;

--select first few records 
select * from tax limit 10;
select * from address limit 10;

--Join the data
SELECT a.folio, sum(b.taxPaid) as totalTaxPaid FROM address a JOIN tax b ON a.folio = b.folio GROUP BY a.folio;

SELECT a.folio, a.streetName, a.postalCode, e.totalTaxPaid from address a join (SELECT b.folio, sum(b.taxPaid) as totalTaxPaid FROM tax b GROUP BY b.folio) e on a.folio = e.folio;


