-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "ea1413be-9e7a-457a-b33d-704607d76ee0",
-- META       "default_lakehouse_name": "OpenTableLakehouse",
-- META       "default_lakehouse_workspace_id": "5fe25e1a-c0df-4de1-80cd-41e83e5ebe9b",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "ea1413be-9e7a-457a-b33d-704607d76ee0"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.read.format("csv").option("header","true").option("delimiter", ";").load("Files/DynamicsData/SalesData/Year=2020/Month=09/Day=27/SalesData_20200927.csv")
-- MAGIC # df now is a Spark DataFrame containing CSV data from "Files/DynamicsData/SalesData/Year=2020/Month=09/Day=27/SalesData_20200927.csv".
-- MAGIC df.createOrReplaceTempView("sales")


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from sales limit 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE dynamicsales
USING delta 
SELECT `Row ID` as RowID, 
       `Order ID` as OrderID,
       `Order Date` as OrderDate,
       `Ship Mode` as ShipMode,
       `Customer ID` as CustomerID,
       `Customer Name` as CustomerName,
       `Segment` as CustomerSegmentName,
       `Country` as CustomerCountryName,
       `City` as CustomerCityName,
       `State` as CustomerStateName,
       CAST(`Postal Code` AS INT) as CustomerPostalCode,
       `Region` as CustomerRegion,
       `Product ID` as ProductID,
       `Category` as ProductCategory,
       `Sub-Category` as ProductSubCategory,
       `Product Name` as ProductName,
       CAST(REPLACE(Sales,',','.') as Decimal(7,2)) AS SalesAmountUSD,
       CAST(Quantity as INT) AS Quantity,
       CAST(REPLACE(Discount,',','.') as Decimal(7,2)) as DiscountAmountUSD,
       CAST(REPLACE(Profit,',','.') as Decimal(7,2)) AS ProfitAmount,
       '2020' AS  Year,
       '09' AS Month,
       '27' as Day
from sales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CustomerSegmentName, sum(SalesAmountUSD) as SalesAmount FROM dynamicsales
GROUP BY CustomerSegmentName

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE HISTORY dynamicsales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC dfday2 = spark.read.format("csv").option("header","true").option("delimiter", ";").load("Files/DynamicsData/SalesData/Year=2020/Month=09/Day=28/SalesData_20200928.csv")
-- MAGIC # df now is a Spark DataFrame containing CSV data from "Files/DynamicsData/SalesData/Year=2020/Month=09/Day=27/SalesData_20200927.csv".
-- MAGIC dfday2.createOrReplaceTempView("salesday2")


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from salesday2 limit 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SET spark.databricks.delta.schema.autoMerge.enabled = true;

insert into dynamicsales
select `Row ID` as RowID, 
       `Order ID` as OrderID,
       `Order Date` as OrderDate,
       `Ship Mode` as ShipMode,
       `Customer ID` as CustomerID,
       `Customer Name` as CustomerName,
       `Segment` as CustomerSegmentName,
       `Country` as CustomerCountryName,
       `City` as CustomerCityName,
       `State` as CustomerStateName,
       CAST(`Postal Code` AS INT) as CustomerPostalCode,
       `Region` as CustomerRegion,
       `Product ID` as ProductID,
       `Category` as ProductCategory,
       `Sub-Category` as ProductSubCategory,
       `Product Name` as ProductName,
       CAST(REPLACE(Sales,',','.') as Decimal(7,2)) AS SalesAmountUSD,
       CAST(Quantity as INT) AS Quantity,
       CAST(REPLACE(Discount,',','.') as Decimal(7,2)) as DiscountAmountUSD,
       CAST(REPLACE(Profit,',','.') as Decimal(7,2)) AS ProfitAmount,
       '2020' AS  Year,
       '09' AS Month,
       '28' as Day,
       RowAdd as ExtraField
from salesday2 


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from dynamicsales limit 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE HISTORY dynamicsales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*) from dynamicsales VERSION AS OF 0

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from dynamicsales 
where customerid = 'CG-12520'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DELETE FROM dynamicSales
where customerid = 'CG-12520'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE HISTORY dynamicsales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from dynamicsales VERSION AS OF 1
where customerid = 'CG-12520'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

VACUUM dynamicsales RETAIN 0 HOURS

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE HISTORY dynamicsales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from validated.dynamicsales VERSION AS OF 1
where customerid = 'CG-12520'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

UPDATE dynamicsales 
SET customername = sha2(customername,256)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from dynamicsales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE EXTENDED dynamicsales


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
