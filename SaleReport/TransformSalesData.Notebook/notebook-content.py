# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "15afd258-567d-4aba-8f58-d35969180b12",
# META       "default_lakehouse_name": "LakehouseDemoDevOps",
# META       "default_lakehouse_workspace_id": "ed6ba6c8-3d48-416a-badb-996fe11d7058"
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
df = (spark.read.format("csv").option("header","true").option("delimiter",";").load("Files/SalesData_20200927.csv")
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

print("this is the new code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
