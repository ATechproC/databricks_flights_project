# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS flights;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights.source;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS flights.bronze CASCADE;