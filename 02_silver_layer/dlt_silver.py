import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

## bookings :

@dlt.view(name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/bookings/data/")\
              .withColumn("amount", F.col("amount").cast("double"))\
              .withColumn("booking_date", F.col("booking_date").cast("timestamp"))\
              .withColumn("_ingested_at", F.current_timestamp())
    return df

@dlt.view(name="trans_bookings")
def trans_bookings():
    df = dlt.read_stream("stage_bookings")\
            .drop("_rescued_data")

    return df

@dlt.table(name="silver_bookings")
def silver_bookings():
    df = dlt.read_stream("trans_bookings")
    return df

# airports :

@dlt.view(name="stage_airports")
def stage_airports():
    df = spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/airports/data/")\
              .withColumn("_ingested_at", F.current_timestamp())
    return df

@dlt.view(name="trans_airports")
def trans_airports():
    df = dlt.read_stream("stage_airports")\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by= F.col("_ingested_at"),
    stored_as_scd_type = 1
)

# passengers :

@dlt.view(name="stage_passengers")
def stage_airports():
    df = spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/passengers/data/")\
              .withColumn("_ingested_at", F.current_timestamp())
    return df

@dlt.view(name="trans_passengers")
def trans_airports():
    df = dlt.read_stream("stage_passengers")\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by= F.col("_ingested_at"),
    stored_as_scd_type = 1
)

# flights :

@dlt.view(name="stage_flights")
def stage_airports():
    df = spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/flights/data/")\
              .withColumn("_ingested_at", F.current_timestamp())
    return df

@dlt.view(name="trans_flights")
def trans_airports():
    df = dlt.read_stream("stage_flights")\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by= F.col("_ingested_at"),
    stored_as_scd_type = 1
)



