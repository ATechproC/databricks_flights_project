# Databricks notebook source
parameters = [
    {"src" : "bookings"},
    {"src" : "airports"},
    {"src" : "flights"},
    {"src" : "passengers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = parameters)
