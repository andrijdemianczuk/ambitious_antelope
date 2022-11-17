# ambitious_antelope
Ambitious Antelope is a project designed to bootstrap data mining an API endpoint returning JSON data with Apache Spark and Delta Lake. This project is also designed to be a showcase on how to approach notebook-driven coding with a more generalized and abstracted approach commonly used in software engineering and application development.

## Introduction
JSON has become the defacto standard for sending plain-text data quickly and seamlessly across networks with pseudo-structure. Although this makes things easy to understand with n-nested structures, it can sometime be difficult to map to a table. In this project we will attempt to do just that. Although this was developed primarily for use in [Databricks](https://www.databricks.com/try-databricks) notebooks, the concepts apply to Jupyter notebooks as well.

## Datasources & Agreements
All test data is sourced from RapidAPI.com under the respective dataset distribution terms, licenses and agreements.

## Getting Started
The Ambitious Antelope project is comprised of several notebooks and scripts to defined and execute the pipelines required for ETL of the data and storage within a Delta format. This project is intended to be run within the Databricks Runtime (www.databricks.com)

## Directory Structure Of This Repo
This repo is broken into a couple of sub-categories to help keep work organized:

Root: Used to home the individual components, README and EULA.
<br/>|-- Projects: Used to home the individual project directories
<br/>|--|-- Testing_Playgound: Used as a general sandbox, mostly for development. This is not production content.
<br/>|--|-- Boundaries_IO_Production: Used as a demonstration of a productionized process using the boundaries_io API call from RapidAPI
<br/>|--|-- IHS_Production: Used as a pre-production delivery for S&P Global Markit IHS data

### 1. Cloning this repo in a Databricks workspace
Cloning this repo to work with the source code is probably the easiest way to work with it. 

The high-level steps on how to do this are as follows:
1. Configure the user settings to allow Git Repos
2. In the Repos section of Databricks, select 'Add Repo'
3. Use the SSL repo address and check clone checkbox to create a copy of this repo
4. Experiment!

Full details on how to clone a GitHub repo in your Databricks workspace can be found [here](https://docs.databricks.com/repos/git-operations-with-repos.html#clone-a-git-repo--other-common-git-operations)
### 2. Configuring and testing external API calls

Getting set up with your own API access is easy and openly available to the public. This demo relies on publically accessible data. The steps to get registered for API access is pretty straightforward:

1. Create and log in to a free account at https://rapidapi.com/
2. Search for boundaries-io in the seach bar
3. Subscribe to the basic tier (free) or better
4. Go to Endpoints -> Carrier Routes -> Query for Carriers Route by Zipcodes
5. Click 'Test Endpoint'
6. Under Code Snippets, select '(Python) Requests' from the dropdown
7. Click 'Copy Code'. We'll use this to replace the code in the notebooks to invoke the call.
### 3. Modifying the notebooks to support the payload schema
You can quickly and easily run the notebooks with most of the structure intact, however you will likely need / want to update the following:

Replace the following notebook code with the python request body from step 2.7 (Above)
```python
url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/carrierRoute/zipcode/98122"
querystring = {"resolution":"8"}
headers = {
	"X-RapidAPI-Key": "{REDACTED}",
	"X-RapidAPI-Host": "vanitysoft-boundaries-io-v1.p.rapidapi.com"
}
res
```

Replace the following with the storage location, table names and database of your choice:
```python
#Create Parameters
c_params = DeltaMgr.init_props("coordinates","{TABLE_NAME}", "{DB_NAME}", "{DBFS_LOCATION}")
p_params = DeltaMgr.init_props("properties","{TABLE_NAME}", "{DB_NAME}", "{DBFS_LOCATION}")
```

### 4. DeltaMgr API Reference
The Delta Manager class has three functions that facilitate use of the object. Once imported, all are available for general purpose.

#### Importing the class for use
```python
#Discrete path relative to repo root
from Projects.Boundaries_IO_Production.DeltaMgr import DeltaMgr

#Or relative to this notebook
from DeltaMgr import DeltaMgr
```

Once imported, the `DeltaMgr` object is in scope and available for use.

#### DeltaMgr.InitProps()
Used to build a collection to hold the delta properties

Usage:
```
variable_name = DeltaMgr.init_props("type":String, "table_name":String, "database":String, "delta_location":String)
```

| variable | data type | required | default value | description |
| ----------- | ----------- |----------- |----------- |----------- |
| type | String | no | "default" | A type identifier used when managing several dataframes |
| table_name | String | no | "default" | The name of the table to be written |
| database | String | no | "default" | The database name where the table will be created |
| delta_location | String | no | "/FileStore/Users/tmp" | The path where the delta files will be written |

Example:

```python
c_params = DeltaMgr.init_props("ipsum","bronze_lorem", "foo", "dbfs:/FileStore/Users/foo/bar/coordinates")
```

#### DeltaMgr.update_delta_fs(df, params)

Used to write a dataframe to the delta location of choice

Usage:

```python
DeltaMgr.update_delta_fs(df:Dataframe, <parameter_dictionary>:dict)
```

| variable | data type | required | default value | description |
| ----------- | ----------- |----------- |----------- |----------- |
| df | Dataframe | yes | "" | The dataframe to be written to the delta file system |
| params | Dictionary | yes | "" | The data dictionary containing the configuration parameters [type, table_name, database, delta_location] |

Example:

```python
DeltaMgr.update_delta_fs(df, c_params)
```

#### DeltaMgr.create_delta_table(df, params, match_col, spark)

Used to register a Delta location as a Delta Table in the current metastore

Usage:

```python
DeltaMgr.create_delta_table(df:Dataframe, <parameter_dictionary>:dict, <unique_column_id>:String, <spark_context>:sc)
```

| variable | data type | required | default value | description |
| ----------- | ----------- |----------- |----------- |----------- |
| df | Dataframe | yes | "" | The dataframe to be registed in the metastore|
| params | Dictionary | yes | "" | The data dictionary containing the configuration parameters [type, table_name, database, delta_location] |
| unique_column_id | String | yes | "" | The name of the column containing the unique identifier - used for merge capabilities |
| spark_context | SC | yes | "" | A reference to the spark context in scope. In most cases `spark` |

Example:

```
DeltaMgr.create_delta_table(df, c_params, "indexCol", spark)
```
