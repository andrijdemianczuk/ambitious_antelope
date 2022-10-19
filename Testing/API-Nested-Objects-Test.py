# Databricks notebook source
import requests

url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/20019"

querystring = {"resolution":"8"}

headers = {
	"X-RapidAPI-Key": "7f9e8c0e92msh2e17bd321b686d2p12ad5djsnf1c096a07ffb",
	"X-RapidAPI-Host": "vanitysoft-boundaries-io-v1.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)

# COMMAND ----------

df = spark.read.json(sc.parallelize([response.text]))
df.show(truncate=False)

# COMMAND ----------

#This is a good example of something we're likely going to have to deal with - heavily nested JSON structures
display(df)
