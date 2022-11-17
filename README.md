# ambitious_antelope
Ambitious Antelope is a project designed to bootstrap data mining for IHS Markit data with Apache Spark and Delta Lake

## Introduction
This is the introduction area

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
