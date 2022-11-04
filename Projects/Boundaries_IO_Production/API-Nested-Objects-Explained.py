# Databricks notebook source
# MAGIC %md
# MAGIC # Courier Logistics API Demo
# MAGIC <img src="https://www.dispatchtrack.com/hubfs/delivery%20logistics.webp" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC 
# MAGIC This notebook is designed to showcase how to make an API call to an external provide and process the information as Delta format.
# MAGIC 
# MAGIC The basic structure is fairly straightforward - a call is made periodically to a authenticated endpoint and a JSON object is returned containing the payload of data. In this example, we are assuming that there is no pre-defined schema and we require a bit of processing and analytics to figure out how best to manage the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design
# MAGIC 
# MAGIC The design we're going to use if fairly straightforward in this example. First, we're going to request a response body from an external API call. Once the response object comes back we'll need to de-serialize it before we can process it and organize the data within our Delta Lake. In so doing, we will first be writing the data to Delta Files in the cloud filesystem and then registering the files as a searchable Delta Table.
# MAGIC <br/>
# MAGIC <hr/>
# MAGIC <img src="https://github.com/andrijdemianczuk/ambitious_antelope/raw/main/Projects/Boundaries_IO_Production/Databricks_call_to_API_example.jpg"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our API call, returning a response object

# COMMAND ----------

import requests

#In reality, we would probably have a list of zip codes to iterate through. Might tackle this later if time permits. Should be pretty easy to build an iterator.
url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/98122"

querystring = {"resolution":"8"}

headers = {
	"X-RapidAPI-Key": "7f9e8c0e92msh2e17bd321b686d2p12ad5djsnf1c096a07ffb",
	"X-RapidAPI-Host": "vanitysoft-boundaries-io-v1.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing calling custom-defined modules for this project

# COMMAND ----------

# MAGIC %md
# MAGIC More details on how to work with arbitrary files and loading encapsulated logic via Python Modules and objects can be found [here](https://docs.databricks.com/repos/work-with-notebooks-other-files.html#refactor-code)

# COMMAND ----------

# Print the Pytrhon path along with all of our locations included
import sys
print("\n".join(sys.path))

# COMMAND ----------

# MAGIC %md
# MAGIC So it looks like `/Workspace/Repos/{User}/{Repo_Name}` is available as part of our Python path which is good. This means that we should be able to interface directly with files adjacent to this notebook, or with files elsewhere within our repository.

# COMMAND ----------

#Note that we can have several classes in a single library - DeltaMgr is the name of both the library and class we want to use.
from Projects.Boundaries_IO_Production.DeltaMgr import DeltaMgr

#test it out
tmp = DeltaMgr.init_props()
print(tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-serializing the object & exploding arrays

# COMMAND ----------

#de-serialize the response
df = spark.read.json(sc.parallelize([response.text]))

# COMMAND ----------

from pyspark.sql.functions import explode

#Pull and explode the array column labeled 'features'
df = df.select(explode(df.features))

# COMMAND ----------

#Review the schema of remaining column and element types. We'll do a quick display just to get a simple 'visual' of the remaining nested structures.
df.printSchema()
display(df.head(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Before we move on, we may want to consider where and when we're creating delta tables. This can give us an opportunity to de-normalize or de-null tables depending on our use cases and volume**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dealing with Structs

# COMMAND ----------

# MAGIC %md
# MAGIC **Dealing with structs is fairly straightforward, but since they're strongly-typed we can use a start-select as a simple iterator to blow out the columns.**

# COMMAND ----------

df = df.select("col.*","*")
df = df.drop(df.col)
df = df.drop(df.type)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Let's have another quick look at the structure**

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **So now we have a clear idea of how we're going to bifurcate our logic; one branch will be for mapping coordinates, the other will be for the route summaries**

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to make a decision; do we write the dataframe out to disk and commit it to Delta as-is or just continue with the logic in memory and create two sub-dataframes? This will largely depend on your use case and differences in your data structures. Our dataset in this example is very flat and we want to normalize it to make good use of it. What we need to consider though is how large the datasets will be and the implications of future joins vs. fast-growing tables.
# MAGIC 
# MAGIC In this example our dataset is small and we're only breaking this out into two tables so we'll opt to break it apart into two distinct datasets early even before we commit to disk. It is important to think how & when we'll need to join the carrier data in this example to the coordinates (not often) so this is probably the better choice.

# COMMAND ----------

# MAGIC %md
# MAGIC **We'll be using h3-index as our unique identifier**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the coordinates dataframe

# COMMAND ----------

coordinates_df = df.select("geometry.*", "properties.h3-index")

# COMMAND ----------

display(coordinates_df)

# COMMAND ----------

#rename potentially damaging column names (e.g., with invalid characters in SQL)
coordinates_df = coordinates_df.withColumnRenamed("h3-index", "h3index")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the properties dataframe

# COMMAND ----------

properties_df = df.select("properties.*")

# COMMAND ----------

display(properties_df)

# COMMAND ----------

#rename potentially damaging column names (e.g., with invalid characters in SQL)
properties_df = properties_df.withColumnRenamed("h3-index", "h3index")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commit the split dataframes to delta tables

# COMMAND ----------

c_params = DeltaMgr.init_props("coordinates","b_carrier_coordinates", "ademianczuk", "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/coordinates")
p_params = DeltaMgr.init_props("properties","b_carrier_properties", "ademianczuk", "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/parameters")

# COMMAND ----------

DeltaMgr.update_delta_fs(coordinates_df, c_params)
DeltaMgr.update_delta_fs(properties_df, p_params)

# COMMAND ----------

#We need to remember to pass the spark context into the function so it can be referenced within
DeltaMgr.create_delta_table(coordinates_df, c_params, "h3index", spark)
DeltaMgr.create_delta_table(properties_df, p_params, "h3index", spark)
