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
# MAGIC ## Our API call, returning a response object

# COMMAND ----------

# MAGIC %md
# MAGIC This is a __*development*__ notebook - in order to 'production-ize' this code we'll need to remove un-necessary functions (mostly displays, schema prints etc.)

# COMMAND ----------

import requests

#In reality, we would probably have a list of zip codes to iterate through. Might tackle this later if time permits. Should be pretty easy to build an iterator.
#url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/20019"
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

#Create reference pointers to both dataframes. Tuples are immutable and more efficient than lists - use them if you're not modifying your collections further. Remember that the tuple is what's immutable, NOT the referenced object(s) - they can still be changed if need-be
# df_tuple = (coordinates_df, properties_df)

# COMMAND ----------

def init_props(type) -> dict:
  return {
    "delta_loc": f"dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/{type}",
    "write_fmt": 'delta',
    "table_name": f'b_carrier_{type}',
    "write_mode": 'overwrite',
    "type": type,
    "database": "ademianczuk"
  }

# COMMAND ----------

#DEBUG ONLY
# spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
# spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")

# COMMAND ----------

def update_delta_fs(df, params):
  df.write \
  .format(params["write_fmt"]) \
  .mode(params["write_mode"]) \
  .save(params["delta_loc"])

# COMMAND ----------

def create_delta_table(df, params):
  
  type = params["type"]
  database = params["database"]
  table_name = params["table_name"]
  delta_loc = params["delta_loc"]
  
  if spark._jsparkSession.catalog().tableExists(database, table_name):
    
    print("table already exists..... appending data")
    df.createOrReplaceTempView(f"vw_{type}")  
    
    spark.sql(f"MERGE INTO {database}.{table_name} USING vw_{type} \
      ON vw_{type}.h3index = {database}.{table_name}.h3index \
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
  
  else:
    print("table does not exist..... creating a new one")
    
    # Write the data to its target.
    spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")

# COMMAND ----------

c_params = init_props("coordinates")
p_params = init_props("properties")

# COMMAND ----------

update_delta_fs(coordinates_df, c_params)
update_delta_fs(properties_df, p_params)

# COMMAND ----------

create_delta_table(coordinates_df, c_params)
create_delta_table(properties_df, p_params)
