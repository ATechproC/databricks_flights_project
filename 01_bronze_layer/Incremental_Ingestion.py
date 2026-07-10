# Databricks notebook source
dbutils.widgets.text("src", "")
src_value = dbutils.widgets.get("src")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "csv")\
          .option("cloudFiles.schemaLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
          .option("cloudFiles.schemaEvolutionMode", "rescue")\
          .load(f"/Volumes/flights/source/raw_data/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
  .option("path", f"/Volumes/flights/bronze/bronze_volume/{src_value}/data/")\
  .trigger(once=True)\
  .start()