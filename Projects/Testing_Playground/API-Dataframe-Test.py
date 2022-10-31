# Databricks notebook source
import requests

url = "https://live-world-data.p.rapidapi.com/"

headers = {
}

response = requests.request("GET", url, headers=headers)

# COMMAND ----------

df = spark.read.json(sc.parallelize([response.text]))
df.show(truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------


