# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import pprint
from synapse.ml.mlflow import get_mlflow_env_config


# the URL could change if the workspace is assigned to a different capacity
url = "https://api.fabric.microsoft.com/v1/workspaces/878900d9-0ecd-4c31-80c5-96d9f8b31b9f/aiskills/7678bea5-ba85-47cb-b9e5-135df4346afc/query/deployment"

configs = get_mlflow_env_config()

headers = {
    "Authorization": f"Bearer {configs.driver_aad_token}",
    "Content-Type": "application/json; charset=utf-8"
}

question = "{userQuestion: \"what is the total sales amount by year?\"}"

response = requests.post(url, headers=headers, data = question)

print("RESPONSE: ", response)

print("")

response = json.loads(response.content)

print(response["executedSQL"])
print(response["result"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
