# Databricks notebook source
catalog = "flights"
target_schema = "gold"
target_object = "fact_bookings"

source_schema = "silver"
source_object = "silver_bookings"

dimensions = [
    {
        "table" : "flights.gold.dim_flight",
        "alias" : "f",
        "join_keys" : [("flight_id", "flight_id")]
    },
    {
        "table" : "flights.gold.dim_airport",
        "alias" : "a",
        "join_keys" : [("airport_id", "airport_id")]
    },
        {
        "table" : "flights.gold.dim_passenger",
        "alias" : "p",
        "join_keys" : [("passenger_id", "passenger_id")]
    },
]

select_columns = ["amount", "booking_date"]

fact_table = "flights.silver.silver_bookings"

cdc_column = "_ingested_at"

backdated_refrech = ""


# COMMAND ----------

if len(backdated_refrech) == 0:
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT MAX({cdc_column}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
else:
    last_load = backdated_refrech

# COMMAND ----------

def generate_incremental_load_query(dimensions, select_columns, last_load):
    fact_alias = "b"

    selected_cols = [f"{fact_alias}.{col}" for col in select_columns]

    join_clauses = []
    for dim in dimensions:
        full_table = dim["table"]
        table_alias = dim["alias"]
        table_name = full_table.split("_")[-1]

        selected_cols.append(f"{table_alias}.{table_name}_sk")

        on_condition = ' AND '.join([f"{fact_alias}.{fk} = {table_alias}.{dk}" for fk, dk in dim["join_keys"]])
    
        join_clause = f"LEFT JOIN {full_table} AS {table_alias} ON {on_condition}"
        join_clauses.append(join_clause)

    where_condition = f"{fact_alias}.{cdc_column} > '{last_load}'"

    format_selected_cols = ", ".join(selected_cols)
    format_join_clauses = " ".join(join_clauses)

    query = f"""
    SELECT {format_selected_cols}
    FROM {fact_table} AS {fact_alias}
    {format_join_clauses}
    WHERE {where_condition}
    """.strip()

    return query

# COMMAND ----------

query = generate_incremental_load_query(dimensions, select_columns, last_load)
print(query)

# COMMAND ----------

df_fact_bookings = spark.sql(query)
display(df_fact_bookings)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Handling UpSert & Merge :

# COMMAND ----------

from delta import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC UpSert Surrogate Keys :

# COMMAND ----------

def merge_conditon_fct(src, tgt):
    surrogate_key_merge = []
    merge_conditions = []
    for dim in dimensions:
        full_table = dim["table"]
        table_name = full_table.split("_")[-1]
        table_alias = dim["alias"]

        surrogate_key = f"{table_name}_sk"
        surrogate_key_merge.append(surrogate_key)

        merge_condition = f"{src}.{surrogate_key} = {tgt}.{surrogate_key}"
        merge_conditions.append(merge_condition)
    
    return " AND ".join(merge_conditions)

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(df_fact_bookings.alias("src"), merge_conditon_fct("src", "tgt"))\
                        .whenMatchedUpdateAll()\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_bookings.write.mode("append").saveAsTable(f"{catalog}.{target_schema}.{target_object}")

print(merge_conditon_fct("src", "tgt"))