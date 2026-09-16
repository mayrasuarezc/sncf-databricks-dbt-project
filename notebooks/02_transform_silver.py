# Databricks notebook source
#DEFINE THE TABLES WE WILL USE IN SILVER, IMPORT WINDOW TO CALCULATE STOPS AND SELECT PER STOP
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "sncf_lakehouse"
schema = "transport"

bronze_trips_table = f"{catalog}.{schema}.bronze_trips"
bronze_routes_table = f"{catalog}.{schema}.bronze_routes"
bronze_stop_times_table = f"{catalog}.{schema}.bronze_stop_times"
bronze_stops_table = f"{catalog}.{schema}.bronze_stops"
bronze_stations_table = f"{catalog}.{schema}.bronze_stations"

# COMMAND ----------

#READ THE TABLES FROM UNITY CATALOG
f_trips = spark.table(bronze_trips_table)
df_routes = spark.table(bronze_routes_table)
df_stop_times = spark.table(bronze_stop_times_table)
df_stops = spark.table(bronze_stops_table)
df_stations = spark.table(bronze_stations_table)

# COMMAND ----------

#SELECT COLUMNS BEFORE JOIN
df_trips_clean = f_trips.select(
    "trip_id",
    "route_id",
    "service_id",
    "trip_headsign",
    "direction_id"
)

df_routes_clean = df_routes.select(
    "route_id",
    "agency_id",
    "route_short_name",
    "route_long_name",
    "route_type"
)

df_stop_times_clean = df_stop_times.select(
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence",
    "stop_headsign",
    "pickup_type",
    "drop_off_type"
)

df_stops_clean = df_stops.select(
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "location_type",
    "parent_station"
)

df_stations_clean = df_stations.select(
    "code_uic",
    "libelle",
    "commune",
    "departement",
    "x_wgs84",
    "y_wgs84"
)

# COMMAND ----------

#CONVERT TYPOS
df_trips_clean = (
    df_trips_clean
    .withColumn("trip_id", F.col("trip_id").cast("string"))
    .withColumn("route_id", F.col("route_id").cast("string"))
)

df_routes_clean = (
    df_routes_clean
    .withColumn("route_id", F.col("route_id").cast("string"))
)

df_stop_times_clean = (
    df_stop_times_clean
    .withColumn("trip_id", F.col("trip_id").cast("string"))
    .withColumn("stop_id", F.col("stop_id").cast("string"))
    .withColumn(
        "stop_sequence",
        F.col("stop_sequence").cast("int")
    )
)

df_stops_clean = (
    df_stops_clean
    .withColumn("stop_id", F.col("stop_id").cast("string"))
)

# COMMAND ----------

#FIRST JOIN ROUTES AND TRIPS
df_trip_routes = (
    df_trips_clean.alias("trips")
    .join(
        df_routes_clean.alias("routes"),
        on=F.col("trips.route_id") ==
           F.col("routes.route_id"),
        how="left"
    )
    .select(
        F.col("trips.trip_id"),
        F.col("trips.route_id"),
        F.col("trips.service_id"),
        F.col("trips.trip_headsign"),
        F.col("trips.direction_id"),
        F.col("routes.agency_id"),
        F.col("routes.route_short_name"),
        F.col("routes.route_long_name"),
        F.col("routes.route_type")
    )
)

display(df_trip_routes.limit(20))

# COMMAND ----------

df_trip_stop_times = (
    df_trip_routes.alias("tr")
    .join(
        df_stop_times_clean.alias("st"),
        on=F.col("tr.trip_id") ==
           F.col("st.trip_id"),
        how="inner"
    )
    .select(
        F.col("tr.trip_id"),
        F.col("tr.route_id"),
        F.col("tr.service_id"),
        F.col("tr.trip_headsign"),
        F.col("tr.route_short_name"),
        F.col("tr.route_long_name"),
        F.col("tr.route_type"),
        F.col("st.arrival_time"),
        F.col("st.departure_time"),
        F.col("st.stop_id"),
        F.col("st.stop_sequence"),
        F.col("st.stop_headsign"),
        F.col("st.pickup_type"),
        F.col("st.drop_off_type")
    )
)

display(df_trip_stop_times.limit(20))

# COMMAND ----------

df_trip_stops = (
    df_trip_stop_times.alias("ts")
    .join(
        df_stops_clean.alias("stops"),
        on=F.col("ts.stop_id") ==
           F.col("stops.stop_id"),
        how="left"
    )
    .select(
        F.col("ts.*"),
        F.col("stops.stop_name"),
        F.col("stops.stop_lat"),
        F.col("stops.stop_lon"),
        F.col("stops.location_type"),
        F.col("stops.parent_station")
    )
)

display(df_trip_stops.limit(20))

# COMMAND ----------

window_trip_order = (
    Window
    .partitionBy("trip_id")
    .orderBy(F.col("stop_sequence").asc())
)

df_trip_stops_ranked = (
    df_trip_stops
    .withColumn(
        "stop_rank",
        F.row_number().over(window_trip_order)
    )
)

display(
    df_trip_stops_ranked
    .filter(F.col("stop_rank") <= 3)
    .limit(30)
)

# COMMAND ----------

# CALCULATE THE FIRST AND LAST STOP
window_trip_first_last = (
    Window
    .partitionBy("trip_id")
    .orderBy(F.col("stop_sequence").asc())
)

df_with_first_stop = (
    df_trip_stops
    .withColumn(
        "first_stop",
        F.first("stop_name").over(window_trip_first_last)
    )
)

# COMMAND ----------

window_trip_last = (
    Window
    .partitionBy("trip_id")
    .orderBy(F.col("stop_sequence").desc())
)

df_trip_complete = (
    df_with_first_stop
    .withColumn(
        "last_stop",
        F.first("stop_name").over(window_trip_last)
    )
)

display(
    df_trip_complete.select(
        "trip_id",
        "route_id",
        "first_stop",
        "last_stop"
    ).dropDuplicates(["trip_id"])
)

# COMMAND ----------

#TO SEE THE SHUFFLE
df_trip_stops.explain("formatted")

# COMMAND ----------

df_trip_complete.explain("formatted")

# COMMAND ----------

from pyspark.sql import functions as F
stop_times_partition_summary = (
    df_stop_times_clean
    .withColumn(
        "_partition_id",
        F.spark_partition_id()
    )
    .groupBy("_partition_id")
    .count()
    .orderBy("_partition_id")
)

display(stop_times_partition_summary)

# COMMAND ----------

#Number of partitions
num_stop_times_partitions = (
    df_stop_times_clean
    .select(F.spark_partition_id().alias("_partition_id"))
    .distinct()
    .count()
)

print(
    "Particiones observadas en stop_times:",
    num_stop_times_partitions
)

# COMMAND ----------

trip_stops_partition_summary = (
    df_trip_stops
    .withColumn(
        "_partition_id",
        F.spark_partition_id()
    )
    .groupBy("_partition_id")
    .count()
    .orderBy("_partition_id")
)

display(trip_stops_partition_summary)

# COMMAND ----------

num_trip_stops_partitions = (
    df_trip_stops
    .select(F.spark_partition_id().alias("_partition_id"))
    .distinct()
    .count()
)

print(
    "Particiones observadas en trip_stops:",
    num_trip_stops_partitions
)

# COMMAND ----------

partition_stats = (
    df_trip_stops
    .withColumn(
        "_partition_id",
        F.spark_partition_id()
    )
    .groupBy("_partition_id")
    .count()
    .agg(
        F.min("count").alias("min_rows"),
        F.max("count").alias("max_rows"),
        F.avg("count").alias("avg_rows")
    )
)

display(partition_stats)

# COMMAND ----------

df_trip_stops_repartitioned = (
    df_trip_stops
    .repartition(32, "trip_id")
)

# COMMAND ----------

repartitioned_summary = (
    df_trip_stops_repartitioned
    .withColumn(
        "_partition_id",
        F.spark_partition_id()
    )
    .groupBy("_partition_id")
    .count()
    .orderBy("_partition_id")
)

display(repartitioned_summary)

# COMMAND ----------

df_trip_stops_repartitioned.explain("formatted")

# COMMAND ----------

print(
    spark.conf.get("spark.sql.shuffle.partitions")
)

# COMMAND ----------

# print(
#     "AQE:",
#     spark.conf.get("spark.sql.adaptive.enabled")
# )

print(
    spark.conf.get("spark.sql.ansi.enabled")
)

# COMMAND ----------

partition_summary = (
    df_trip_stops_repartitioned
    .withColumn(
        "_partition_id",
        F.spark_partition_id()
    )
    .groupBy("_partition_id")
    .count()
    .orderBy("_partition_id")
)

display(partition_summary)

# COMMAND ----------

silver_trip_stops_table = (
    "sncf_lakehouse.transport.silver_trip_stops"
)

(
    df_trip_complete.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_trip_stops_table)
)