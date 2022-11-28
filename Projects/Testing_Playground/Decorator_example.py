# Databricks notebook source
from Projects.Boundaries_IO_Production.UcExt import UcExt

# COMMAND ----------

@UcExt.decorator
def sampleFunction():
  print("hello")

sampleFunction()

# COMMAND ----------


