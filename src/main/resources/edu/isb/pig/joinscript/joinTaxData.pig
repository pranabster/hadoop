
--========================================================================================================================================================================================================================
 --Clean up environment 
--========================================================================================================================================================================================================================

--delete the pig output path - /user/ubuntu/pig/output
fs -rm -r pig/output

--========================================================================================================================================================================================================================
--Load Dataset for different use cases
--========================================================================================================================================================================================================================

--Load comma separated tax data from HDFS  path /user/ubuntu/pig/input/tax
taxInput = LOAD '/user/ubuntu/pig/input/tax' USING PigStorage(',') AS (PID:chararray, LEGAL_TYPE:chararray, FOLIO:chararray, LAND_COORDINATE:chararray, ZONE_NAME:chararray, ZONE_CATEGORY:chararray, PLAN:chararray, CURRENT_LAND_VALUE:double, TAX_ASSESSMENT_YEAR:int, TAX_PAID:double);

--Load comma separated Address data from HDFS  path /user/ubuntu/pig/input/address
address = LOAD '/user/ubuntu/pig/input/address' USING PigStorage(',') AS (FOLIO:chararray, STREET_NAME:chararray, PROPERTY_POSTAL_CODE:chararray);

--Load tab separated Address data from HDFS  path /user/ubuntu/pig/input/tsv
address1 = LOAD '/user/ubuntu/pig/input/tsv' AS (FOLIO:chararray, STREET_NAME:chararray, PROPERTY_POSTAL_CODE:chararray);

--Load comma separated tax data from HDFS  path /user/ubuntu/pig/input/tax/2014.csv
taxInputUnionDataSet1 = LOAD '/user/ubuntu/pig/input/tax/2014.csv' USING PigStorage(',') AS (PID:chararray, LEGAL_TYPE:chararray, FOLIO:chararray, LAND_COORDINATE:chararray, ZONE_NAME:chararray, ZONE_CATEGORY:chararray, PLAN:chararray, CURRENT_LAND_VALUE:double, TAX_ASSESSMENT_YEAR:int, TAX_PAID:double);

--Load comma separated tax data from HDFS  path /user/ubuntu/pig/input/tax/2015.csv
taxInputUnionDataSet2 = LOAD '/user/ubuntu/pig/input/tax/2015.csv' USING PigStorage(',') AS (PID:chararray, LEGAL_TYPE:chararray, FOLIO:chararray, LAND_COORDINATE:chararray, ZONE_NAME:chararray, ZONE_CATEGORY:chararray, PLAN:chararray, CURRENT_LAND_VALUE:double, TAX_ASSESSMENT_YEAR:int, TAX_PAID:double);


--========================================================================================================================================================================================================================
-- union two datasets
-- taxInputUnionDataSet1 and taxInputUnionDataSet2  and the record count to varify the total numbef of records. 
-- Pig does not union data set in its real mathemetical sense. It simply appends the dataset. 
--========================================================================================================================================================================================================================

grp1 = GROUP taxInputUnionDataSet1 ALL;
taxInputUnionDataSet1Count = FOREACH grp1 GENERATE COUNT(taxInputUnionDataSet1);
dump taxInputUnionDataSet1Count;

grp2 = GROUP taxInputUnionDataSet2 ALL;
taxInputUnionDataSet2Count = FOREACH grp2 GENERATE COUNT(taxInputUnionDataSet2);
dump taxInputUnionDataSet2Count;

taxInputUnion = union taxInputUnionDataSet1, taxInputUnionDataSet2;

grp3 = GROUP taxInputUnion ALL;
postUnionDataSetCount = FOREACH grp3 GENERATE COUNT(taxInputUnion);
dump postUnionDataSetCount;

--========================================================================================================================================================================================================================
-- Group tax data by folio and then Aggregrate total tax paid 
--========================================================================================================================================================================================================================

--group the taxes paid in different years by FOLIO
groupedTaxInput = group taxInput by FOLIO;


aggregrateGroupedTaxInput = foreach groupedTaxInput {
	generate group as FOLIO, SUM(taxInput.TAX_PAID) AS TOTAL_TAX_PAID;
};

--describe aggregrateGroupedTaxInput;
--first5 = limit aggregrateGroupedTaxInput 5;
--dump first5 ;

--========================================================================================================================================================================================================================
-- Join two datasets - 
--========================================================================================================================================================================================================================

--join the taxPaid aggregrateGroupedTaxInput with the Address relation 
joinedTaxOutPut = join address by FOLIO, aggregrateGroupedTaxInput by FOLIO;


--Show the first 5 records of the output
--describe joinedTaxOutPut;
--first5Records = limit joinedTaxOutPut 5;
--dump first5Records;


--========================================================================================================================================================================================================================
-- Order by Query
--========================================================================================================================================================================================================================

--Orderby property postal code
joinedTaxOutPutOrderByPostalCode = ORDER joinedTaxOutPut BY PROPERTY_POSTAL_CODE;

--Show the first 5 records of the order by output
--describe joinedTaxOutPutOrderByPostalCode;
--first5Records = limit joinedTaxOutPutOrderByPostalCode 5;
--dump first5Records;

--========================================================================================================================================================================================================================
-- Store the result
--========================================================================================================================================================================================================================

--Store finalOutPut data in HDFS path /user/ubuntu/pig/output
--store joinedTaxOutPut into '/user/ubuntu/pig/output' USING PigStorage(',');
store joinedTaxOutPutOrderByPostalCode into '/user/ubuntu/pig/output' USING PigStorage(',');


--View the content of the output folder
--fs -cat pig/output/part*
