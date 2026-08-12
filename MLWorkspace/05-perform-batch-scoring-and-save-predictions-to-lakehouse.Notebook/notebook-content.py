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
# META     "environment": {
# META       "environmentId": "083b8463-5369-946a-400b-c7b2dee84bec",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Module 5: Perform batch scoring and save predictions to lakehouse

# MARKDOWN ********************

# We start with mounting the default lakehouse, as in modules 2-4, and setting configurations to optimize performance, as in module 1.

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true") # Enable VOrder write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Read a random sample of cleansed data from lakehouse for the year 2016 and month 3

# CELL ********************

SEED = 1234 # Random seed
input_df = spark.read.format("delta").load("Tables/nyctaxi_prep")\
            .filter("puYear = 2016 AND puMonth = 3")\
            .sample(True, 0.01, seed=SEED) ## Sampling data to reduce execution time for this tutorial

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get the trained and registered model to generate predictions

# CELL ********************

import mlflow
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from synapse.ml.core.platform import *
from synapse.ml.lightgbm import LightGBMRegressor

## Define run_uri to fetch the model
run_uri = "runs:/ecae32f4-58d3-4dd6-9062-b72c5ec3ccba/nyctaxi_tripduration_lightgbm"
loaded_model = mlflow.spark.load_model(run_uri, dfs_tmpdir="Files/tmp/mlflow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Run model transform on the input dataframe to generate predictions and remove unnecessary vector features created for model training

# CELL ********************

# Generate predictions by applying model transform on the input dataframe
predictions = loaded_model.transform(input_df)
cols_toremove = ['storeAndFwdFlagIdx', 'timeBinsIdx', 'vendorIDIdx', 'paymentTypeIdx', 'vendorIDEnc',
 'rateCodeIdEnc', 'paymentTypeEnc', 'weekDayEnc', 'pickupHourEnc', 'storeAndFwdFlagEnc', 'timeBinsEnc', 'features','weekDayNameIdx',
 'pickupHourIdx', 'rateCodeIdIdx', 'weekDayNameEnc']
output_df = predictions.withColumnRenamed("prediction", "predictedtripDuration").drop(*cols_toremove)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save predictions to lakehouse delta table

# CELL ********************

table_name = "nyctaxi_pred"
output_df.write.mode("overwrite").format("delta").save(f"Tables/{table_name}")
print(f"Output Predictions saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Preview predicted dataframe

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM nyctaxi_pred LIMIT 20

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
