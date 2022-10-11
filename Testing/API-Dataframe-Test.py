# Databricks notebook source
import requests

url = "https://live-world-data.p.rapidapi.com/"

headers = {
	"X-RapidAPI-Key": "7f9e8c0e92msh2e17bd321b686d2p12ad5djsnf1c096a07ffb",
	"X-RapidAPI-Host": "live-world-data.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

print(response.text)

# COMMAND ----------

df = response.text
