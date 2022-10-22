# Databricks notebook source
# MAGIC %md
# MAGIC ## Our API call, returning a response object

# COMMAND ----------

import requests

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

# MAGIC %md
# MAGIC ### Build the properties dataframe

# COMMAND ----------

properties_df = df.select("properties.*")

# COMMAND ----------

display(properties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commit the split dataframes to delta tables

# COMMAND ----------

#Create reference pointers to both dataframes. Tuples are immutable and more efficient than lists - use them if you're not modifying your collections further.
df_tuple = (coordinates_df, properties_df)

# COMMAND ----------

for i in df_tuple:
  i.printSchema()

# COMMAND ----------


