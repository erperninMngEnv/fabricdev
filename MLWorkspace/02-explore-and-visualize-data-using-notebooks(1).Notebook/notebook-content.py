# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Module 2: Explore and Visualize Data
# 
# In this module we will use seaborn, a Python data visualization library that provides a high-level interface for building visuals on dataframes and arrays. You can learn more about seaborn [here](https://seaborn.pydata.org/).

# MARKDOWN ********************

# Please add the lakehouse you created earlier as the default lakehouse in this notebook. 

# MARKDOWN ********************

# #### Read delta table from lakehouse and create a pandas dataframe on a random sample of the data
# Note: For the purpose of minimizing runtime in this tutorial, We are using a 1/1000 sample to explore and visualize ingested data

# CELL ********************

data = spark.read.format("delta").load("Tables/nyctaxi_raw")
SEED = 1234
sampled_df = data.sample(True, 0.001, seed=SEED).toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Import visualization libraries and set figure config

# CELL ********************

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
sns.set_theme(style="whitegrid", palette="tab10", rc = {'figure.figsize':(9,6)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Visual 1: Distribution of trip duration(minutes) on linear and logarithmic scale

# CELL ********************

## Compute trip duration(in minutes) on the sample using pandas
sampled_df['tripDuration'] = (sampled_df['tpepDropoffDateTime'] - sampled_df['tpepPickupDateTime']).astype('timedelta64[m]')
sampled_df = sampled_df[sampled_df["tripDuration"] > 0]

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
sns.histplot(ax=axes[0],data=sampled_df,
            x="tripDuration",
            stat="count",
            discrete=True).set(title='Distribution of trip duration(minutes)')
sns.histplot(ax=axes[1],data=sampled_df,
            x="tripDuration",
            stat="count", 
            log_scale= True).set(title='Distribution of trip duration(log scale)')
axes[1].xaxis.set_major_formatter(mticker.ScalarFormatter())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 2: Lets create bins to visualize duration of trips better

# CELL ********************

## Create bins for tripDuration column
sampled_df.loc[sampled_df['tripDuration'].between(0, 10, 'both'), 'durationBin'] = '< 10 Mins'
sampled_df.loc[sampled_df['tripDuration'].between(10, 30, 'both'), 'durationBin'] = '10-30 Mins'
sampled_df.loc[sampled_df['tripDuration'].between(30, 60, 'both'), 'durationBin'] = '30-60 Mins'
sampled_df.loc[sampled_df['tripDuration'].between(60, 120, 'right'), 'durationBin'] = '1-2 Hrs'
sampled_df.loc[sampled_df['tripDuration'].between(120, 240, 'right'), 'durationBin'] = '2-4 Hrs'
sampled_df.loc[sampled_df['tripDuration'] > 240, 'durationBin'] = '> 4 Hrs'

# Plot histogram using the binned column
sns.histplot(data=sampled_df, x="durationBin", stat="count", discrete=True, hue = "durationBin")
plt.title("Trip Distribution by Duration Bins")
plt.xlabel('Trip Duration')
plt.ylabel('Frequency')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 3: Visualize the distribution of tripDuration and tripDistance and classify by passengerCount

# CELL ********************

sns.scatterplot(data=sampled_df, x="tripDistance", y="tripDuration", hue="passengerCount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 4: Visualize distribution of passengercount per trip

# CELL ********************

sns.histplot(data=sampled_df, x="passengerCount", stat="count", discrete=True)
plt.title("Distribution of passenger count")
plt.xlabel('No. of Passengers')
plt.ylabel('Number of trips')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 5:  Create boxplots to visualize the distribution of tripDuration by passenger count
# A boxplot is a useful tool to understand the variability, symmetry, and outliers of the data.
# - In first figure lets visualize tripDuration without removing any outliers
# - In the second figure we are removing trips with duration greater than ~3 hours and zero passengers.

# CELL ********************

# The threshold was calculated by evaluating mean trip duration (~15 minutes) + 3 standard deviations (58 minutes), i.e. roughly 3 hours.
fig, axes = plt.subplots(1, 2, figsize=(18, 6))
sns.boxplot(ax=axes[0], data=sampled_df, x="passengerCount", y="tripDuration").set(title='Distribution of Trip duration by passengerCount')
sampleddf_clean = sampled_df[(sampled_df["passengerCount"] > 0) & (sampled_df["tripDuration"] <= 180)]
sns.boxplot(ax=axes[1], data=sampleddf_clean, x="passengerCount", y="tripDuration").set(title='Distribution of Trip duration by passengerCount (outliers removed)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 6: Analyze the relationship of tripDuration and fareAmount classified by paymentType and VendorId using a scatterplot/subplots

# CELL ********************

f, axes = plt.subplots(1, 2, figsize=(18, 6))
sns.scatterplot(ax =axes[0], data=sampled_df, x="fareAmount", y="tripDuration",  hue="paymentType")
sns.scatterplot(ax =axes[1],data=sampled_df, x="fareAmount", y="tripDuration",  hue="vendorID")
plt.title("Distribution of tripDuration by fareAmount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 7: Analyze the frequency of the taxi trips by hour of the day

# CELL ********************

sampled_df['hour'] = sampled_df['tpepPickupDateTime'].dt.hour
sampled_df['dayofweek'] = sampled_df['tpepDropoffDateTime'].dt.dayofweek
sampled_df['dayname'] = sampled_df['tpepDropoffDateTime'].dt.day_name()
sns.histplot(data=sampled_df, x="hour", stat="count", discrete=True, kde=True)
plt.title("Distribution by Hour of the day")
plt.xlabel('Hours')
plt.ylabel('Count of trips')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 8: Analyze average taxi trip duration by hour and day of the week using a heatmap

# CELL ********************

pv_df = sampled_df[sampled_df["tripDuration"]<180]\
        .groupby(["hour","dayname"]).mean("tripDuration")\
        .reset_index().pivot("hour", "dayname", "tripDuration")
sns.heatmap(pv_df,annot=True,fmt='.2f', cmap="Blues").set(xlabel=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Visual 9: Create a Correlation plot 
# A correlation plot is a useful tool for exploring the relationships among numerical variables in a dataset. It displays the data points for each pair of variables as a scatterplot, and also calculates the correlation coefficient for each pair. The correlation coefficient indicates how strongly and in what direction the variables are related. A positive correlation means that the variables tend to increase or decrease together, while a negative correlation means that they tend to move in opposite directions.

# CELL ********************

cols_to_corr = ['tripDuration','fareAmount', 'passengerCount', 'tripDistance', 'extra', 'mtaTax',
       'improvementSurcharge', 'tipAmount', 'hour',"dayofweek"]
sns.heatmap(data = sampled_df[cols_to_corr].corr(),annot=True,fmt='.3f', cmap="Blues")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Summary of observations from data exploration:
# 
# 1) Some trips in the sample have passenger count of 0 but most trips have a passenger count between 1-6.
# 2) tripDuration column has outliers with a comparatively small number of trips having trip duration of greater than 3 hours.
# 3) The outliers for TripDuration are specifically for vendorId 2.
# 4) Some trips have zero tripdistance and hence can be treated as cancelled and filtered out.
# 5) A small number of trips have no passengers (0) and hence can be filtered out.
# 6) fareAmount column contains negative outliers which can be removed from training.
# 6) The number of trips start rising around 16:00 hours and peaks between 18:00 - 19:00 hours.

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
