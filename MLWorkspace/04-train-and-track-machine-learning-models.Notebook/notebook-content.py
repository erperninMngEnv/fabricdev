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

# ### Train and register a machine learning model
# 
# Once a model is trained, register the trained model, and log hyperparameters used and evaluation metrics using Fabric's native integration with the MLflow framework.
# 
# [MLflow](https://mlflow.org/docs/latest/index.html) is an open source platform for managing the machine learning lifecycle with features like Tracking, Models, and Model Registry. MLflow is natively integrated with Fabric Data Science Experience.

# MARKDOWN ********************

# #### Import mlflow and create an experiment to log the run

# CELL ********************

# Create Experiment to Track and register model with mlflow
import mlflow
print(f"mlflow lbrary version: {mlflow.__version__}")
EXPERIMENT_NAME = "nyctaxi_tripduration"
mlflow.set_experiment(EXPERIMENT_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Read Cleansed data from lakehouse delta table (saved in module 3)

# CELL ********************

SEED = 1234
# note: From the perspective of the tutorial, we are sampling training data to speed up the execution.
training_df = spark.read.format("delta").load("Tables/nyctaxi_prep").sample(fraction = 0.5, seed = SEED)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Perform random split to get train and test datasets and define categorical and numeric features

# CELL ********************

TRAIN_TEST_SPLIT = [0.75, 0.25]
train_df, test_df = training_df.randomSplit(TRAIN_TEST_SPLIT, seed=SEED)

# Cache the dataframes to improve the speed of repeatable reads
train_df.cache()
test_df.cache()

print(f"train set count:{train_df.count()}")
print(f"test set count:{test_df.count()}")

categorical_features = ["storeAndFwdFlag","timeBins","vendorID","weekDayName","pickupHour","rateCodeId","paymentType"]
numeric_features = ['passengerCount', "tripDistance"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define the steps to perform additional feature engineering and train the model using Spark ML pipelines and Microsoft SynapseML library
# You can learn more about Spark ML pipelines [here](https://spark.apache.org/docs/latest/ml-pipeline.html), and SynapseML is documented [here](https://microsoft.github.io/SynapseML/docs/about/)
# 
# The algorithm used for this tutorial, [LightGBM](https://lightgbm.readthedocs.io/en/v3.3.2/) is a fast, distributed, high performance gradient boosting framework based on decision tree algorithms. It is an open source project developed by Microsoft and supports regression, classification and many other machine learning scenarios. Its main advantages are faster training speed, lower memory usage, better accuracy, and support for distributed learning.

# CELL ********************

from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from synapse.ml.core.platform import *
from synapse.ml.lightgbm import LightGBMRegressor

# Define a pipeline steps for training a LightGBMRegressor regressor model
def lgbm_pipeline(categorical_features,numeric_features, hyperparameters):
    # String indexer
    stri = StringIndexer(inputCols=categorical_features, 
                        outputCols=[f"{feat}Idx" for feat in categorical_features]).setHandleInvalid("keep")
    # encode categorical/indexed columns
    ohe = OneHotEncoder(inputCols= stri.getOutputCols(),  
                        outputCols=[f"{feat}Enc" for feat in categorical_features])
    
    # convert all feature columns into a vector
    featurizer = VectorAssembler(inputCols=ohe.getOutputCols() + numeric_features, outputCol="features")

    # Define the LightGBM regressor
    lgr = LightGBMRegressor(
        objective = hyperparameters["objective"],
        alpha = hyperparameters["alpha"],
        learningRate = hyperparameters["learning_rate"],
        numLeaves = hyperparameters["num_leaves"],
        labelCol="tripDuration",
        numIterations = hyperparameters["iterations"],
        dataTransferMode="bulk"
    )
    # Define the steps and sequence of the Spark ML pipeline
    ml_pipeline = Pipeline(stages=[stri, ohe, featurizer, lgr])
    return ml_pipeline


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define Training Hyperparameters
# Hyperparameters are the parameters that you can change to control how a machine learning model is trained. Hyperparameters can affect the speed, quality and accuracy of the model. Some common methods to find the best hyperparameters are by testing different values, using a grid or random search, or using a more advanced optimization technique.
# The hyperparameters for the lightgbm model in this tutorial have been pre-tuned using a distributed gridsearch run using [hyperopt](https://github.com/hyperopt/hyperopt)

# MARKDOWN ********************

# #### Model Run 1: Using default lightgbm hyperparameters

# CELL ********************

# Default hyperparameters for LightGBM Model
LGBM_PARAMS = {"objective":"regression",
    "alpha":0.9,
    "learning_rate":0.1,
    "num_leaves":31,
    "iterations":100}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Fit the defined pipeline on the training dataframe and generate predictions on the test dataset

# CELL ********************

if mlflow.active_run() is None:
    mlflow.start_run()
run = mlflow.active_run()
print(f"Active experiment run_id: {run.info.run_id}")
lg_pipeline = lgbm_pipeline(categorical_features,numeric_features,LGBM_PARAMS)
lg_model = lg_pipeline.fit(train_df)

# Get Predictions
lg_predictions = lg_model.transform(test_df)
## Caching predictions to run model evaluation faster
lg_predictions.cache()
print(f"Prediction run for {lg_predictions.count()} samples")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Compute Model Statistics for evaluating performance of the trained LightGBMRegressor model

# CELL ********************

from synapse.ml.train import ComputeModelStatistics
lg_metrics = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="tripDuration", scoresCol="prediction"
).transform(lg_predictions) 

lg_metrics=lg_metrics.withColumnRenamed('R^2', 'R2')
display(lg_metrics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Register the trained LightGBMRegressor model using MLflow

# CELL ********************

from mlflow.models.signature import ModelSignature 
from mlflow.types.utils import _infer_schema
import json

# Define a function to register a spark model
def register_spark_model(run, model, model_name,signature,metrics, hyperparameters):
        # log the model, parameters and metrics
        mlflow.spark.log_model(model, artifact_path = model_name, signature=signature, registered_model_name = model_name, dfs_tmpdir="Files/tmp/mlflow") 
        mlflow.log_params(hyperparameters) 
        mlflow.log_metrics(metrics) 
        model_uri = f"runs:/{run.info.run_id}/{model_name}" 
        print(f"Model saved in run{run.info.run_id}") 
        print(f"Model URI: {model_uri}")
        return model_uri

# Define Signature object 
sig = ModelSignature(inputs=_infer_schema(train_df.select(categorical_features + numeric_features)), 
                     outputs=_infer_schema(train_df.select("tripDuration"))) 

ALGORITHM = "lightgbm" 
model_name = f"{EXPERIMENT_NAME}_{ALGORITHM}"

# Create a 'dict' object that contains values of metrics
lg_metrics_dict = json.loads(lg_metrics.toJSON().first())

# Call model register function
model_uri = register_spark_model(run = run,
                                model = lg_model, 
                                model_name = model_name, 
                                signature = sig, 
                                metrics = lg_metrics_dict, 
                                hyperparameters = LGBM_PARAMS)
mlflow.end_run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Model Run 2: Using tuned lightgbm hyperparameters and remove paymentType 
# Since paymentType is usually selected at the end of a trip, we hypothesize that it shouldn't be useful to predict trip duration.

# CELL ********************

# Tuned hyperparameters for LightGBM Model
TUNED_LGBM_PARAMS = {"objective":"regression",
    "alpha":0.08373361416254149,
    "learning_rate":0.0801709918703746,
    "num_leaves":92,
    "iterations":200}

# Remove paymentType
categorical_features.remove("paymentType")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Fit the lightgbm pipeline with tuned hyperparameter on the training dataframe and generate predictions on the test dataset

# CELL ********************

if mlflow.active_run() is None:
    mlflow.start_run()
run = mlflow.active_run()
print(f"Active experiment run_id: {run.info.run_id}")
lg_pipeline_tn = lgbm_pipeline(categorical_features,numeric_features,TUNED_LGBM_PARAMS)
lg_model_tn = lg_pipeline_tn.fit(train_df)

# Get Predictions
lg_predictions_tn = lg_model_tn.transform(test_df)
## Caching predictions to run model evaluation faster
lg_predictions_tn.cache()
print(f"Prediction run for {lg_predictions_tn.count()} samples")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lg_metrics_tn = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="tripDuration", scoresCol="prediction"
).transform(lg_predictions_tn).withColumnRenamed('R^2', 'R2')
display(lg_metrics_tn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Register the trained LightGBMRegressor model using MLflow

# CELL ********************

# Define Signature object 
sig_tn = ModelSignature(inputs=_infer_schema(train_df.select(categorical_features + numeric_features)), 
                     outputs=_infer_schema(train_df.select("tripDuration")))

# Create a 'dict' object that contains values of metrics
lg_metricstn_dict = json.loads(lg_metrics_tn.toJSON().first())

model_uri = register_spark_model(run = run,
                                model = lg_model_tn, 
                                model_name = model_name, 
                                signature = sig_tn, 
                                metrics = lg_metricstn_dict, 
                                hyperparameters = TUNED_LGBM_PARAMS)
mlflow.end_run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Note: if you do not see your model artifact in the workspace, please make sure to refresh your browser.

# MARKDOWN ********************

# #### You will need the below run_uri to execute the next tutorial

# CELL ********************

print(f"Please copy this run_uri: {model_uri}")

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
