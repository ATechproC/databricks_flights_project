# Databricks notebook source
# MAGIC %md
# MAGIC # Handling Incremental Ingestion :

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

catalog = "flights"

target_schema = "gold"
target_object = "dim_flight"

source_schema = "silver"
source_object = "silver_flights"

cdc_col = "_ingested_at"
key_cols = ["flight_id"]
surrogate_key = "flight_sk"

backdated_refrech = ""

# COMMAND ----------

catalog = "flights"

target_schema = "gold"
target_object = "dim_passenger"

source_schema = "silver"
source_object = "silver_passengers"

cdc_col = "_ingested_at"
key_cols = ["passenger_id"]
surrogate_key = "passenger_sk"

backdated_refrech = ""

# COMMAND ----------

catalog = "flights"

target_schema = "gold"
target_object = "dim_airport"

source_schema = "silver"
source_object = "silver_airports"

cdc_col = "_ingested_at"
key_cols = ["airport_id"]
surrogate_key = "airport_sk"

backdated_refrech = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Last Load :

# COMMAND ----------

if len(backdated_refrech) == 0:
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT MAX({cdc_col}) FROM {catalog}.{source_schema}.{source_object}").collect()[0][0]
    else :
        last_load = "1900-01-01 00:00:00"
else :
    last_load = backdated_refrech

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {catalog}.{source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")
display(df_src.limit(10))

# COMMAND ----------

incremental_load = ', '.join(key_cols)
initial_load = ', '.join([f"'' AS {col}" for col in key_cols])

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    df_tgt = spark.sql(f"SELECT {incremental_load}, {surrogate_key}, update_date, create_date FROM {catalog}.{target_schema}.{target_object}")
else:
    df_tgt = spark.sql(f"SELECT {initial_load}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS update_date, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS create_date")

display(df_tgt.limit(10))

# COMMAND ----------

df_join = df_src.join(df_tgt, key_cols, "left")
display(df_join.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### OLD VS NEW RECORDS :

# COMMAND ----------

df_join.createOrReplaceTempView("v_join")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OLD RECORDS :

# COMMAND ----------

df_old = spark.sql(f"SELECT * FROM v_join WHERE {surrogate_key} IS NOT NULL")
display(df_old.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### NEW RECORDS :

# COMMAND ----------

df_new = spark.sql(f"SELECT * FROM v_join WHERE {surrogate_key} IS NULL")
display(df_new.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare DF_OLD :

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog}.{target_schema}.{target_schema}").collect()[0][0]
    df_new_enr = df_new.withColumn("create_date", F.current_timestamp())\
                   .withColumn("update_date", F.current_timestamp())\
                   .withColumn(surrogate_key, max_surrogate_key + F.lit(1) + F.monotonically_increasing_id())
else:
    df_new_enr = df_new.withColumn("create_date", F.current_timestamp())\
                   .withColumn("update_date", F.current_timestamp())\
                   .withColumn(surrogate_key, F.lit(1)+F.monotonically_increasing_id())

display(df_new_enr.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare DF_OLD :

# COMMAND ----------

df_old_enr = df_old.withColumn("update_date", F.current_timestamp())
display(df_old_enr.limit(10))
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union DF_OLD & DF_NEW :

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)
display(df_union.limit(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **_`Handling UpSert : (MERGER)`_**

# COMMAND ----------

from delta import DeltaTable

# COMMAND ----------

merge_condition = " AND ".join([f"src.{key} = tgt.{key}" for key in key_cols])

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(df_union.alias("src"), merge_condition)\
            .whenMatchedUpdateAll(condition=f"src.{cdc_col} > tgt.{cdc_col}")\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_union.write.format("delta").mode("append").saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------


spark.sql(f"SELECT * FROM {catalog}.{target_schema}.{target_object} LIMIT 10").display()