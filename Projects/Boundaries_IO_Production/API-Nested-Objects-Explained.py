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

# MAGIC %md
# MAGIC ### Testing with a public API
# MAGIC For this example, we will be making a point of using a public API and API key. This API is available for limited general use [here](https://rapidapi.com/VanitySoft/api/boundaries-io-1/). You may need to register to access this API (limited to 50 calls/day at the free tier), but it serves as a good example. This API was selected because the response object contains nested JSON which is relevant to serve as an example of how to deal with multi-tiered payloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using common Python classes
# MAGIC PySpark on Databricks includes a number of commonly used libraries pre-installed in [pipenv](https://pypi.org/project/pipenv/). We're going to make use of the requests module since it provides a simple way to invoke a request object that is sent off to the API endpoint. The response payload comes back as an object which we'll label as 'response'. The response object contains not only the payload itself but meta-information as well including important data on the success/failure of the call.

# COMMAND ----------

import requests


url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/98122"
querystring = {"resolution":"8"}

#The headers typically include our authentication parameters. In reality, we would likely store these in the Databricks secrets scope rather than storing them in plain sight like this.
headers = {
	"X-RapidAPI-Key": "7f9e8c0e92msh2e17bd321b686d2p12ad5djsnf1c096a07ffb",
	"X-RapidAPI-Host": "vanitysoft-boundaries-io-v1.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

#Print the status code of the API call. We're looking for '200' here indicating that the response came back successfully from the API. The response object also contains stack trace info in the event of a failure for debugging purposes.
print(response.status_code)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.pngall.com/wp-content/uploads/10/Attention-PNG-Free-Image.png" width=100 />
# MAGIC <br/>
# MAGIC 
# MAGIC ### Using Databricks Secrets
# MAGIC 
# MAGIC Rather than exposing the API key in plain text like we do above, it might be preferred to use a [Databricks secret](https://docs.databricks.com/security/secrets/index.html) instead. This is a much more secure way to store and utilize sensitive parameters while working with code that's versioned with a code repository.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluating the response
# MAGIC 
# MAGIC Before we move on, let's have a quick look at the response payload. This will give us a good representation of what we are working with and how to best manage the data coming back. We can do this by simply printing the response text to a cell.

# COMMAND ----------

print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Okay, so this response may look complicated, but it does tell us a few important things. 
# MAGIC 1. First, the entire structure is more-or-less human-readable. We're not worrying about responses in byte code or anything like that which is good. 
# MAGIC 1. Second we can see that the opening and closing tags are curly braces and sub-delimiters with square brackets which tells us there's a good likelihood this is well-formed JSON
# MAGIC 1. This is presented as one large text object at this point which means we'll have to de-serialize this by the interpreter in order to convert it to readable JSON.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing calling custom-defined modules for this project

# COMMAND ----------

# MAGIC %md
# MAGIC More details on how to work with arbitrary files and loading encapsulated logic via Python Modules and objects can be found [here](https://docs.databricks.com/repos/work-with-notebooks-other-files.html#refactor-code)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <!-- img src="https://duws858oznvmq.cloudfront.net/menskool_Blog_5da39a6f12.jpg" width=500 / -->
# MAGIC 
# MAGIC ### Code re-use
# MAGIC 
# MAGIC It's important to look for opportunites to re-use and abstract code that's common. One such function that is often used repeatedly is the process by which we write to our delta lake and create delta tables from those files in our cloud storage buckets. There are two ways we can abstract and encapsulate logic blocks into other files and repositories.
# MAGIC 
# MAGIC 1. Creating Python modules in packages for distribution via PyPi - this involves creating Python Egg files (the old way) or Python Wheel files (the new way) and either publishing them to a public/private Python package repo (or sideloading them directly at the cluster level) or...
# MAGIC 2. Loading arbitrary files that are formatted as class libraries (which we'll be doing here.)
# MAGIC 
# MAGIC The point though is to re-use code that can be re-used for common functions as much as possible. We can do this with good coding practices and through the use of object references (which we'll see when we pass our spark context around).
# MAGIC <hr />
# MAGIC <img src="https://github.com/andrijdemianczuk/ambitious_antelope/raw/main/Projects/Boundaries_IO_Production/Loading_Git_Python_Modules.jpg" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Identifying our import locations
# MAGIC 
# MAGIC By printing our Python reference path variable, we can see all of the locations that the Python runtime has access to. Specifically we are looking for something along the lines of the following:
# MAGIC 
# MAGIC ```
# MAGIC /Workspace/Repos/{databricks_user}/{Repo}/{Path_to_Project}
# MAGIC /Workspace/Repos/{databricks_user}/{Repo}
# MAGIC ```

# COMMAND ----------

# Print the Pytrhon path along with all of our locations included
import sys
print("\n".join(sys.path))

# COMMAND ----------

# MAGIC %md
# MAGIC So it looks like `/Workspace/Repos/{databricks_user}/{Repo}` is available as part of our Python path which is good. This means that we should be able to interface directly with files adjacent to this notebook, or with files elsewhere within our repository.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing modules from other repos to work with directly
# MAGIC It's also important to note that your modules don't necessarily have to exist in the same repo as the databricks notebook and function reference within Git. You can easily add repo locations to your Python path with the following example block of code:
# MAGIC ```python
# MAGIC import sys
# MAGIC import os
# MAGIC  
# MAGIC # In the command below, replace <username> with your Databricks user name.
# MAGIC sys.path.append(os.path.abspath('/Workspace/Repos/<username>/supplemental_files'))
# MAGIC  
# MAGIC # You can now import Python modules from the supplemental_files repo.
# MAGIC # import lib
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC So let's import our Delta Manager class which will help us when managing our delta files and tables. We're just going to invoke the `init_props()` function as a test because it doesn't require any input parameters.

# COMMAND ----------

#Note that we can have several classes in a single library - DeltaMgr is the name of both the library and class we want to use.
from Projects.Boundaries_IO_Production.DeltaMgr import DeltaMgr

#test it out
tmp = DeltaMgr.init_props()
print(tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we know that we have our class library available to us to help with Delta file and table management, we can continue working on the API response object.

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-serializing the object & exploding arrays

# COMMAND ----------

# MAGIC %md
# MAGIC ### De-serializing the response object
# MAGIC 
# MAGIC **Deserialization is the process of reconstructing a data structure or object from a series of bytes or a string in order to instantiate the object for consumption. This is the reverse process of serialization, i.e., converting a data structure or object into a series of bytes for storage or transmission across devices.** Since we have our response payload as a text attribute of the response object, we'll need to de-serialize it for use and submit it to Spark to do it's thing. Spark will take take the input and properly distribute it across the RDD in scope for the Spark Context. Fortunately for us, this can all be done with two commands that are concatenated in a single line.
# MAGIC <br />
# MAGIC <br />
# MAGIC * spark.read.json() will deserialize the string response back into a JSON-typed object
# MAGIC * sc.parallelize() will handle the distribution of the object across the RDD. This is represented by a dataframe that's instanciated to represent the data structure.

# COMMAND ----------

#de-serialize the response
df = spark.read.json(sc.parallelize([response.text]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling arrays
# MAGIC Working with arrays in the context of dataframes is pretty straightforward. All we need is a pyspark function called `explode` which will handle all of the column mapping for us. Arrays are a bit looser in terms of structure than structs (which are essentially pseudo objects) so we can leverage built-in functionality to help us manage them and map them properly.
# MAGIC 
# MAGIC There are a couple of types of maps we can apply to arrays to get the desired effect:
# MAGIC * explode() - ignores any elements that are null or empty (e.g.,[])
# MAGIC * explore_outer() - returns `null` for elements that are null or empty
# MAGIC * posexplode() - similar to `explode` but also contains information about the element's position in the array or map
# MAGIC * posexplode_outer() - similar to `explode_outer` but also contains information about the element's position in the array or map
# MAGIC 
# MAGIC For the sake of our needs, we only need the `explode()` function. For more details on row mapping and managing null values using the other three functions, documentation and a good explanation can be found [here](https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/).

# COMMAND ----------

from pyspark.sql.functions import explode

#Pull and explode the array column labeled 'features'
df = df.select(explode(df.features))

# COMMAND ----------

# MAGIC %md
# MAGIC Now when we look at the resulting schema of our dataframe we can see that we've blown out our 'features' column (which only contains a single element called 'col') into a column with distinct rows for each entry.

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
# MAGIC Looking at our dataframe now, we can see that our root element is a struct that contains three child elements:
# MAGIC - Geometry: contains all of our polygon coordinates
# MAGIC - Properties: contains all of our carrier route data
# MAGIC - Type: a simple label for the data point (we're not going to use this column so we'll drop it when we create our new version of the dataframe)
# MAGIC 
# MAGIC We want to map these three columns to a new version of the dataframe which is pretty easy to do with a star-select command on the dataframe columns.
# MAGIC 
# MAGIC **Dealing with structs is fairly straightforward, but since they're strongly-typed we can use a star-select as a simple iterator to map the columns.**

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
# MAGIC ### Analyzing the resulting dataframe
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Manager Reference
# MAGIC 
# MAGIC The Delta Manager class has four functions that facilitate use of the object. Once imported, all are available for general purpose.
