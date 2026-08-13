# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aafc296e-d541-4cc2-a38c-72371d92d476",
# META       "default_lakehouse_name": "MLLakehouse",
# META       "default_lakehouse_workspace_id": "50f0dd61-c5c4-4167-aca4-67dee02eb27d",
# META       "known_lakehouses": [
# META         {
# META           "id": "aafc296e-d541-4cc2-a38c-72371d92d476"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Module 3: Perform Data Cleansing and preparation using Apache Spark

# MARKDOWN ********************

# Please add the lakehouse you created earlier as the default lakehouse in this notebook.

# MARKDOWN ********************

# #### Load NYC taxi Data from lakehouse delta table

# CELL ********************

nytaxi_df = spark.read.format("delta").load("Tables/nyctaxi_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Get Summary Statistics of all the columns using Spark dataframe summary

# CELL ********************

display(nytaxi_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(nytaxi_df.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Clean data and add additional derived columns
# 
# **<u>Add derived Columns</u>**
# - pickupDate - convert datetime to date for visualizations and reporting.
# - weekDay - day number of the week
# - weekDayName - day names abbreviated.
# - dayofMonth - day number of the month
# - pickupHour - hour of pickup time
# - tripDuration - representing duration in minutes of the trip.
# - timeBins - Binned time of the day
# 
# 
# **<u>Filter Conditions</u>** <p>
# - fareAmount is between 0 and 100 
# - tripDistance greater than 0, remove outstation trips(outliers) tripDistance>100.
# - tripDuration is less than 3 hours (180 minutes) 
# - passengerCount is between 1 and 8.
# - startLat, startLon, endLat, endLon are not NULL.
# 
# The inputs for setting these conditions were derived from EDA performed in Module 2. Specifically,
# - fareAmount == 0 can be treated as 'dirty data' ("free ride"), and 100 threshold is picked to ignore "outliers" based on reviewing the diagram (it also corresponds to 0.9997 quantile of values in that column)
# - we consider tripDistance==0 to be 'cancelled trip', therefore of no interest
# - tripDuration threshold was calculated by evaluating mean trip duration (~15 minutes) + 3 standard deviations (58 minutes), i.e. roughly 3 hours.
# - we decided to disregard trips with 0 passengers as meaningless, and we haven't seen any trips with over 8 passengers
# - intuitively, and confirmed by visualizations, the trips and their properties are not evenly distributed throughout the day, hence it's important to consider 'timeBins'


# CELL ********************

from pyspark.sql.functions import col,when, dayofweek, date_format, hour,unix_timestamp, round, dayofmonth, lit
nytaxidf_prep = nytaxi_df.withColumn('pickupDate', col('tpepPickupDateTime').cast('date'))\
                            .withColumn("weekDay", dayofweek(col("tpepPickupDateTime")))\
                            .withColumn("weekDayName", date_format(col("tpepPickupDateTime"), "EEEE"))\
                            .withColumn("dayofMonth", dayofweek(col("tpepPickupDateTime")))\
                            .withColumn("pickupHour", hour(col("tpepPickupDateTime")))\
                            .withColumn("tripDuration", (unix_timestamp(col("tpepDropoffDateTime")) - unix_timestamp(col("tpepPickupDateTime")))/60)\
                            .withColumn("timeBins", when((col("pickupHour") >=7) & (col("pickupHour")<=10) ,"MorningRush")\
                            .when((col("pickupHour") >=11) & (col("pickupHour")<=15) ,"Afternoon")\
                            .when((col("pickupHour") >=16) & (col("pickupHour")<=19) ,"EveningRush")\
                            .when((col("pickupHour") <=6) | (col("pickupHour")>=20) ,"Night"))\
                            .filter("""fareAmount > 0 AND fareAmount < 100 and tripDistance > 0 AND tripDistance < 100 
                                    AND tripDuration > 0 AND tripDuration <= 180 
                                    AND passengerCount > 0 AND passengerCount <= 8
                                    AND startLat IS NOT NULL AND startLon IS NOT NULL AND endLat IS NOT NULL AND endLon IS NOT NULL""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save Cleansed and prepared data to lakehouse delta table

# CELL ********************

table_name = "nyctaxi_prep"
nytaxidf_prep.write.mode("overwrite").format("delta").save(f"Tables/{table_name}")
print(f"Spark dataframe saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
