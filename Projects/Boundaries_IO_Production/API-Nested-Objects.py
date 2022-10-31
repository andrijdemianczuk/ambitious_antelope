# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Dependencies
import requests
from pyspark.sql.functions import explode

# COMMAND ----------

# DBTITLE 1,Supporting Functions
def init_props(type) -> dict:
  return {
    "delta_loc": f"dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/CarrierRoutes/data/{type}",
    "write_fmt": 'delta',
    "table_name": f'b_carrier_{type}',
    "write_mode": 'overwrite',
    "type": type,
    "database": "ademianczuk"
  }

def update_delta_fs(df, params):
  df.write \
  .format(params["write_fmt"]) \
  .mode(params["write_mode"]) \
  .save(params["delta_loc"])

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
c_params = init_props("coordinates")
p_params = init_props("properties")

#Update the Delta File System
update_delta_fs(coordinates_df, c_params)
update_delta_fs(properties_df, p_params)

#Create Delta tables from the File System
create_delta_table(coordinates_df, c_params)
create_delta_table(properties_df, p_params)
