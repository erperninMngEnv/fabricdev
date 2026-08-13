# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d1d1d67e-45c8-4e60-a2fe-5eaa72c79eb5",
# META       "default_lakehouse_name": "SalesReportLakehouse",
# META       "default_lakehouse_workspace_id": "b58bbd7c-1ba0-4a4a-a53e-4d962ec92ad5",
# META       "known_lakehouses": [
# META         {
# META           "id": "d1d1d67e-45c8-4e60-a2fe-5eaa72c79eb5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Importing libraries

# CELL ********************

import uuid
from pyspark.sql.functions import lit, col, udf, regexp_replace
from pyspark.sql.types import DecimalType, StringType, IntegerType, DateType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Defining structure of data. 

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = (spark.read.format("csv").option("header","true").option("delimiter",";").load("Files/salesdata/SalesData_20200927.csv")
        .select(col("Row ID").alias("RowID"), 
                col("Order ID").alias("OrderID"),
                col("Order Date").alias("OrderDate").cast(DateType()),
                col("Ship Mode").alias("ShipMode"),
                col("Customer ID").alias("CustomerID"),
                col("Customer Name").alias("CustomerName"),
                col("Segment").alias("CustomerSegmentName"),
                col("Country").alias("CustomerCountryName"),
                col("City").alias("CustomerCityName"),
                col("State").alias("CustomerStateName"),
                col("Postal Code").alias("CustomerPostalCode"),
                col("Region").alias("CustomerRegionName"),
                col("Product ID").alias("ProductID"),
                col("Category").alias("ProductCategoryName"),
                col("Sub-Category").alias("ProductSubCategoryName"),
                col("Product Name").alias("ProductName"),
                col("Sales").alias("SalesAmountUSD"),
                col("Quantity").alias("Quantity").cast(IntegerType()),
                col("Discount").alias("DiscountAmountUSD"),
                col("Profit").alias("ProfitAmount") )
        .withColumn("SalesAmountUSD", regexp_replace("SalesAmountUSD",',','.').cast(DecimalType(8,2)))
        .withColumn("DiscountAmountUSD", regexp_replace("DiscountAmountUSD",',','.').cast(DecimalType(8,2)))
        .withColumn("ProfitAmount", regexp_replace("ProfitAmount",',','.').cast(DecimalType(8,2)))
      )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Storing silver data format.

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("SalesData")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
