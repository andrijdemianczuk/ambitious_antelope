# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Dependencies
import requests
from pyspark.sql.functions import explode
from DeltaMgr import DeltaMgr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our API call, returning a response object

# COMMAND ----------

url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/98122"
querystring = {"resolution":"8"}
headers = {
	"X-RapidAPI-Key": "7f9e8c0e92msh2e17bd321b686d2p12ad5djsnf1c096a07ffb",
	"X-RapidAPI-Host": "vanitysoft-boundaries-io-v1.p.rapidapi.com"
}
response = requests.request("GET", url, headers=headers, params=querystring)

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-serializing the object & exploding arrays

# COMMAND ----------

#de-serialize the response
df = spark.read.json(sc.parallelize([response.text]))

# COMMAND ----------

#Pull and explode the array column labeled 'features'
df = df.select(explode(df.features))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dealing with Structs

# COMMAND ----------

df = df.select("col.*","*")
df = df.drop(df.col)
df = df.drop(df.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the coordinates dataframe

# COMMAND ----------

coordinates_df = df.select("geometry.*", "properties.h3-index")
coordinates_df = coordinates_df.withColumnRenamed("h3-index", "h3index")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the properties dataframe

# COMMAND ----------

properties_df = df.select("properties.*")
properties_df = properties_df.withColumnRenamed("h3-index", "h3index")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commit the split dataframes to delta tables

# COMMAND ----------

#Create Parameters
c_params = DeltaMgr.init_props("coordinates","b_carrier_coordinates", "ademianczuk", "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/coordinates")
p_params = DeltaMgr.init_props("properties","b_carrier_properties", "ademianczuk", "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/parameters")

#Update the Delta File System
DeltaMgr.update_delta_fs(coordinates_df, c_params)
DeltaMgr.update_delta_fs(properties_df, p_params)

#Create Delta tables from the File System
DeltaMgr.create_delta_table(coordinates_df, c_params, "h3index", spark)
DeltaMgr.create_delta_table(properties_df, p_params, "h3index", spark)
