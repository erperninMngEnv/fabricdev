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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Module 1: Ingest data into lakehouse using Spark
# **Lakehouse**:
# A lakehouse is a collection of files/folders/tables that represent a database over a data lake used by 
# the Spark engine and SQL engine for big data processing and that includes enhanced capabilities for 
# ACID transactions when using the open-source Delta formatted tables.
# 
# **Delta Lake**: Delta Lake is an open-source storage layer that brings ACID transactions, scalable metadata management, and batch and streaming data processing to Apache Spark. A Delta Lake table is a data table format that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata management.

# MARKDOWN ********************

# #### Connect to Azure Open Datasets Container and read NYC Taxi yellow cab dataset
# [Azure Open Datasets](https://learn.microsoft.com/en-us/azure/open-datasets/overview-what-are-open-datasets) are curated public datasets that you can use to add scenario-specific features to machine learning solutions for more accurate models. Open Datasets are in the cloud on Microsoft Azure Storage and can be accessed by a variety of methods including, Apache Spark, REST API, Datafactory and other tools.

# CELL ********************

# Azure storage access info for open datasets yellow cab
storage_account = "azureopendatastorage"
container = "nyctlc"

sas_token = r"" # Blank since container is Anonymous access

# Set Spark config to access  blob storage
spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (container, storage_account),sas_token)

dir = "yellow"
year = 2016
months = "1,2,3,4"
wasbs_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{dir}"
df = spark.read.parquet(wasbs_path)

# Filter data by year and months
filtered_df = df.filter(f"puYear = {year} AND puMonth IN ({months})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Spark dataframe to lakehouse delta table

# MARKDOWN ********************

# **Enable Vorder and Optimized Delta Write**
# 
# - **VOrder**: Fabric includes Microsoft's VOrder engine. VOrder writer optimizes the Delta Lake parquet files resulting in 3x-4x compression improvement and up to 10x performance acceleration over Delta Lake files not optimized using VOrder while still maintaining full Delta Lake and PARQUET format compliance.<p>
# - **Optimize write**: Spark in Fabric includes an Optimize Write feature that reduces the number of files written and targets to increase individual file size of the written data. It dynamically optimizes files during write operations generating files with a default 128 MB size. The target file size may be changed per workload requirements using configurations.
# 
# These configs can be applied at a session level(as spark.conf.set in a notebook cell) as demonstrated in the following code cell, or at workspace level which is applied automatically to all spark sessions created in the workspace. The workspace level Apache Spark configuration can be set at:
# - _Workspace settings >> Data Engineering/Sceience >> Spark Compute >> Spark Properties >> Add_


# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true") # Enable VOrder write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "nyctaxi_raw"
filtered_df.write.mode("overwrite").format("delta").save(f"Tables/{table_name}")
print(f"Spark dataframe saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
