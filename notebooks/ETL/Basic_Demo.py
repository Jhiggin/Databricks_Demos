# Databricks notebook source
# MAGIC %md
# MAGIC # Demo - Common ETL Patterns Using DataBricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a mount point to our datalake to make accessing data easier

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://<container>@<storageaccount>.blob.core.windows.net",
    mount_point="/mnt/data",
    extra_configs={
        "fs.azure.account.key.<storageaccount>.blob.core.windows.net": dbutils.secrets.get(
            scope="key-vault-secret", key="dlkey"
        )
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract data from Data lake to Spark Dataframe

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

# set the data lake file location:
file_location = (
    "/mnt/data/in/*.csv"
)

# Define schema
schema = StructType(
    [
        StructField("trip_duration", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("stop_time", TimestampType(), True),
        StructField("start_station_id", IntegerType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_latitude", DoubleType(), True),
        StructField("start_station_longitude", DoubleType(), True),
        StructField("end_station_id", IntegerType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_latitude", DoubleType(), True),
        StructField("end_station_longitude", DoubleType(), True),
        StructField("bike_id", IntegerType(), True),
        StructField("user_type", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("gender", IntegerType(), True),
    ]
)

# load the data from the csv file to a data frame
df_citi_bike_data = (
    spark.read.option("header", "true")
    .schema(schema)
    .option("delimiter", ",")
    .csv(file_location)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a date field off of start_time to partition by later

# COMMAND ----------

from pyspark.sql.functions import to_date

df_citi_bike_data = df_citi_bike_data.withColumn('Rental_Date', to_date('start_time'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the dataframe to review what data was pulled

# COMMAND ----------

display(df_citi_bike_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from SQL Server

# COMMAND ----------

from pyspark.sql import SparkSession

# Retrieve the username and password from the secret scope
HostName = dbutils.secrets.get("datalake_scope", "sql-host")
DbName = dbutils.secrets.get("datalake_scope", "sql-db")
UserName = dbutils.secrets.get("datalake_scope", "sql-user")
Password = dbutils.secrets.get("datalake_scope", "sql-password")

# Define your SQL query
sql_query = """
    Select * from DimDate
"""

df_date = (spark.read
  .format("sqlserver")
  .option("host", HostName)
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", UserName)
  .option("password", Password)
  .option("database", DbName)
  .option("query", sql_query) # (if schemaName not provided, default to "dbo")
  .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display date dataframe

# COMMAND ----------

display(df_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading data to Delta Files

# COMMAND ----------

# Writing data to persistent storage (for example, Delta Lake)
bronze_delta_location = "/mnt/data/bronze"

df_citi_bike_data.write.format("delta").mode("overwrite").save(
    bronze_delta_location + "/citi_bike_data"
)

# COMMAND ----------

bronze_delta_location = "/mnt/data/bronze"

display(dbutils.fs.ls(bronze_delta_location + "/citi_bike_data/_delta_log"))

# COMMAND ----------

create_database_command = f"""
    CREATE DATABASE IF NOT EXISTS Citi_Bike;
"""

spark.sql(create_database_command)

# COMMAND ----------

# SQL command to create a Delta table

drop_table_command = f"""
    DROP TABLE IF EXISTS Citi_Bike.raw_bike_data
"""

create_table_command = f"""
    CREATE TABLE Citi_Bike.raw_bike_data
    USING DELTA
    LOCATION '{bronze_delta_location}/citi_bike_data'
"""

# Execute SQL command
spark.sql(drop_table_command)
spark.sql(create_table_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL citi_bike.raw_bike_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   citi_bike.raw_bike_data

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower
from pyspark.sql.functions import monotonically_increasing_id

df_start_stations = spark.sql(
    "SELECT start_station_id as Station_ID, start_station_name as Station_Name, start_station_latitude as Station_Latitude, start_station_longitude as Station_Longitude FROM citi_bike.raw_bike_data"
)

df_end_stations = spark.sql(
    "SELECT end_station_id as Station_ID, end_station_name as Station_Name, end_station_latitude as Station_Latitude, end_station_longitude as Station_Longitude FROM citi_bike.raw_bike_data"
)

df_stations = df_start_stations.union(df_end_stations)

df_stations = df_stations.distinct()

df_bikes = spark.sql("SELECT bike_id from citi_bike.raw_bike_data")

df_bikes = df_bikes.distinct()

# Add an key column to each dataframe to act as a Surrogate Key.
df_stations = df_stations.withColumn("Stations_key", monotonically_increasing_id())
df_bikes = df_bikes.withColumn("Bike_key", monotonically_increasing_id())

# COMMAND ----------

df_stations.groupBy(df_stations.columns).count().filter("count > 1").show(
    truncate=False
)

df_bikes.groupBy(df_bikes.columns).count().filter("count > 1").show(truncate=False)

# COMMAND ----------

# Writing data to persistent storage (for example, Delta Lake)
delta_location = "/mnt/data/silver"

df_stations.write.format("delta").mode("overwrite").save(
    delta_location + "/dim_stations"
)

df_bikes.write.format("delta").mode("overwrite").save(
    delta_location + "/dim_bikes"
)

df_date.write.format("delta").mode("overwrite").save(
    delta_location + "/dim_date"
)

# COMMAND ----------

# SQL command to create a Delta table

drop_table_command = f"""
    DROP TABLE IF EXISTS citi_bike.dim_stations
"""

create_table_command = f"""
    CREATE TABLE citi_bike.dim_stations
    USING DELTA
    LOCATION '{delta_location}/dim_stations'
"""

# Execute SQL command
spark.sql(drop_table_command)
spark.sql(create_table_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike.dim_stations

# COMMAND ----------

# SQL command to create a Delta table

drop_table_command = f"""
    DROP TABLE IF EXISTS citi_bike.dim_bikes
"""

create_table_command = f"""
    CREATE TABLE citi_bike.dim_bikes
    USING DELTA
    LOCATION '{delta_location}/dim_bikes'
"""

# Execute SQL command
spark.sql(drop_table_command)
spark.sql(create_table_command)

# COMMAND ----------

# SQL command to create a Delta table

drop_table_command = f"""
    DROP TABLE IF EXISTS citi_bike.dim_date
"""

create_table_command = f"""
    CREATE TABLE citi_bike.dim_date
    USING DELTA
    LOCATION '{delta_location}/dim_date'
"""

# Execute SQL command
spark.sql(drop_table_command)
spark.sql(create_table_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   station_id,
# MAGIC   COUNT(*) AS count
# MAGIC FROM
# MAGIC   citi_bike.dim_stations
# MAGIC GROUP BY
# MAGIC   station_id
# MAGIC HAVING
# MAGIC   COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Stations_key,
# MAGIC   Station_id,
# MAGIC   Station_name,
# MAGIC   Station_latitude,
# MAGIC   Station_longitude
# MAGIC FROM
# MAGIC   citi_bike.dim_stations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dim_bikes

# COMMAND ----------

df_trips = spark.sql(" \
     SELECT  d.datekey as Rental_Date_Key, bd.trip_duration, s.stations_key as Start_Station_Key, es.stations_key as End_Station_Key, b.bike_key as Bike_Key  \
     FROM citi_bike.raw_bike_data as bd \
     JOIN citi_bike.dim_date d ON bd.rental_date = d.FullDateAlternateKey \
     JOIN citi_bike.dim_bikes b ON bd.bike_id = b.bike_id \
     LEFT JOIN citi_bike.dim_stations s on bd.start_station_id = s.station_id \
     LEFT JOIN citi_bike.dim_stations es on bd.end_station_id = es.station_id \
")

# COMMAND ----------

display(df_trips)

# COMMAND ----------

df_trips.write.format("delta").mode("overwrite").save(
    delta_location + "/fact_trips"
)

# COMMAND ----------

# SQL command to create a Delta table

drop_table_command = f"""
    DROP TABLE IF EXISTS citi_bike.fact_trips
"""

create_table_command = f"""
    CREATE TABLE citi_bike.fact_trips
    USING DELTA
    LOCATION '{delta_location}/fact_trips'
"""

# Execute SQL command
spark.sql(drop_table_command)
spark.sql(create_table_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from citi_bike.fact_trips
