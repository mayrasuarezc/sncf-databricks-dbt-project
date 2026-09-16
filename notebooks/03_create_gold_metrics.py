# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "sncf_lakehouse"
schema = "transport"

silver_trip_stops_table = (
    f"{catalog}.{schema}.silver_trip_stops"
)

df_silver = spark.table(silver_trip_stops_table)

display(df_silver.limit(20))

# COMMAND ----------

df_gold_station_traffic = (
    df_silver
    .groupBy("stop_id", "stop_name")
    .agg(
        F.countDistinct("trip_id").alias(
            "number_of_trips"
        ),
        F.countDistinct("route_id").alias(
            "number_of_routes"
        )
    )
    .orderBy(
        F.col("number_of_trips").desc()
    )
)

display(df_gold_station_traffic.limit(20))

# COMMAND ----------

gold_station_table = (
    f"{catalog}.{schema}.gold_station_traffic"
)

(
    df_gold_station_traffic.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_station_table)
)

# COMMAND ----------

df_gold_route_summary = (
    df_silver
    .groupBy(
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type"
    )
    .agg(
        F.countDistinct("trip_id").alias(
            "number_of_trips"
        ),
        F.countDistinct("stop_id").alias(
            "number_of_stops"
        ),
        F.count("*").alias(
            "number_of_stop_records"
        )
    )
    .orderBy(
        F.col("number_of_trips").desc()
    )
)

display(df_gold_route_summary.limit(20))

# COMMAND ----------

gold_route_table = f"{catalog}.{schema}.gold_route_summary"

(
    df_gold_route_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_route_table)
)

# COMMAND ----------

window_trip = (
    Window
    .partitionBy("trip_id")
    .orderBy(
        F.col("stop_sequence").asc()
    )
)

df_trip_boundaries = (
    df_silver
    .withColumn(
        "row_number_ascending",
        F.row_number().over(window_trip)
    )
)

# COMMAND ----------

window_trip_desc = (
    Window
    .partitionBy("trip_id")
    .orderBy(
        F.col("stop_sequence").desc()
    )
)

df_trip_boundaries = (
    df_trip_boundaries
    .withColumn(
        "row_number_descending",
        F.row_number().over(window_trip_desc)
    )
)

# COMMAND ----------

df_gold_trip_summary = (
    df_trip_boundaries
    .groupBy(
        "trip_id",
        "route_id",
        "route_short_name"
    )
    .agg(
        F.max(
            F.when(
                F.col("row_number_ascending") == 1,
                F.col("stop_name")
            )
        ).alias("first_stop"),
        F.max(
            F.when(
                F.col("row_number_descending") == 1,
                F.col("stop_name")
            )
        ).alias("last_stop"),
        F.max("stop_sequence").alias(
            "number_of_stops"
        )
    )
)

display(df_gold_trip_summary.limit(20))

# COMMAND ----------

gold_trip_table = (
    f"{catalog}.{schema}.gold_trip_summary"
)

(
    df_gold_trip_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_trip_table)
)

# COMMAND ----------

spark.sql(f"""
SHOW TABLES IN {catalog}.{schema}
""").show(truncate=False)

# COMMAND ----------

df_gold_station_traffic.explain("formatted")

# COMMAND ----------

display(
    df_gold_station_traffic
    .filter(F.col("number_of_trips") > 0)
    .orderBy(
        F.col("number_of_trips").desc()
    )
    .limit(20)
)

# COMMAND ----------

display(
    df_gold_station_traffic.filter(
        F.col("stop_id").isNull()
        | F.col("stop_name").isNull()
    )
)

# COMMAND ----------

gold_tables = [
    "gold_station_traffic",
    "gold_route_summary",
    "gold_trip_summary"
]

for table_name in gold_tables:
    full_table_name = (
        f"sncf_lakehouse.transport.{table_name}"
    )
    
    print(full_table_name)
    display(
        spark.table(full_table_name).limit(5)
    )

# COMMAND ----------

gold_station_table = (
    "sncf_lakehouse.transport.gold_station_traffic"
)

(
    df_gold_station_traffic.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_station_table)
)

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "sncf_lakehouse"
schema = "transport"

df_station_reference = (
    spark.table(
        f"{catalog}.{schema}.bronze_stations"
    )
    .select(
        "code_uic",
        "libelle",
        "commune",
        "departement",
        "x_wgs84",
        "y_wgs84"
    )
)

# COMMAND ----------

display(
    df_station_reference.select(
        "libelle",
        "commune",
        "x_wgs84",
        "y_wgs84"
    ).limit(20)
)

# COMMAND ----------

df_station_reference.select(
    F.min("x_wgs84").alias("min_x"),
    F.max("x_wgs84").alias("max_x"),
    F.min("y_wgs84").alias("min_y"),
    F.max("y_wgs84").alias("max_y")
).show()

# COMMAND ----------

df_dim_stations = (
    df_station_reference
    .withColumn(
        "longitude",
        F.col("x_wgs84").cast("double")
    )
    .withColumn(
        "latitude",
        F.col("y_wgs84").cast("double")
    )
    .withColumnRenamed(
        "libelle",
        "station_name"
    )
    .withColumnRenamed(
        "commune",
        "city"
    )
    .withColumnRenamed(
        "departement",
        "department"
    )
    .filter(
        F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )
    .dropDuplicates(["code_uic"])
)

# COMMAND ----------

df_gold_station_traffic = spark.table(
    f"{catalog}.{schema}.gold_station_traffic"
)

display(
    df_gold_station_traffic.select(
        "stop_id",
        "stop_name",
        "number_of_trips",
        "number_of_routes"
    ).limit(20)
)

# COMMAND ----------

print("TRAFFIC stop_id examples")
display(
    df_gold_station_traffic
    .select("stop_id", "stop_name")
    .limit(20)
)

print("STATIONS code_uic examples")
display(
    df_dim_stations
    .select("code_uic", "station_name", "city", "department")
    .limit(20)
)

# COMMAND ----------

# df_station_map = (
#     df_gold_station_traffic.alias("traffic")
#     .join(
#         df_dim_stations.alias("stations"),
#         F.col("traffic.stop_id") ==
#         F.col("stations.code_uic"),
#         "left"
#     )
#     .select(
#         F.col("traffic.stop_id"),
#         F.col("traffic.stop_name"),
#         F.col("traffic.number_of_trips"),
#         F.col("traffic.number_of_routes"),
#         F.col("stations.station_name"),
#         F.col("stations.city"),
#         F.col("stations.department"),
#         F.col("stations.latitude"),
#         F.col("stations.longitude")
#     )
# )

df_station_map = (
    df_gold_station_traffic.alias("traffic")
    .join(
        df_dim_stations.alias("stations"),
        F.regexp_extract(
            F.col("traffic.stop_id"),
            r"(\d{8})$",
            1
        ) == F.col("stations.code_uic").cast("string"),
        "left"
    )
    .select(
        F.col("traffic.stop_id").alias("stop_id"),
        F.col("traffic.stop_name").alias("stop_name"),
        F.col("traffic.number_of_trips").alias("number_of_trips"),
        F.col("traffic.number_of_routes").alias("number_of_routes"),
        F.coalesce(
            F.col("stations.station_name"),
            F.col("traffic.stop_name")
        ).alias("station_name"),
    F.coalesce(
            F.col("stations.city"),
            F.lit("Unknown / Outside France")
        ).alias("city"),

        F.coalesce(
            F.col("stations.department"),
            F.lit("Not available")
        ).alias("department"),
        F.col("stations.latitude").cast("double").alias("latitude"),
        F.col("stations.longitude").cast("double").alias("longitude")
    )
)

# COMMAND ----------

display(
    df_station_map.select(
        "stop_id",
        "stop_name",
        "number_of_trips",
        "number_of_routes",
        "station_name",
        "city",
        "department",
        "latitude",
        "longitude"
    ).limit(20)
)

# COMMAND ----------

# df_station_map.select(
#     F.count("*").alias("total_rows"),
#     F.sum(
#         F.col("latitude").isNull().cast("int")
#     ).alias("missing_latitude"),
#     F.sum(
#         F.col("longitude").isNull().cast("int")
#     ).alias("missing_longitude")
# ).show()

df_station_map.select(
    F.count("*").alias("total_rows"),
    F.sum(F.col("stop_name").isNull().cast("int")).alias("missing_stop_name"),
    F.sum(F.col("station_name").isNull().cast("int")).alias("missing_station_name"),
    F.sum(F.col("city").isNull().cast("int")).alias("missing_city"),
    F.sum(F.col("department").isNull().cast("int")).alias("missing_department"),
    F.sum(F.col("latitude").isNull().cast("int")).alias("missing_latitude"),
    F.sum(F.col("longitude").isNull().cast("int")).alias("missing_longitude")
).show()

# COMMAND ----------

display(
    df_station_map.select(
        "stop_id",
        "stop_name",
        "station_name",
        "city",
        "department",
        "latitude",
        "longitude",
        "number_of_trips",
        "number_of_routes"
    ).limit(20)
)

# COMMAND ----------

powerbi_station_table = (
    f"{catalog}.{schema}.gold_powerbi_station"
)

(
    df_station_map
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(powerbi_station_table)
)

# COMMAND ----------

print(powerbi_station_table)

display(
    spark.table(powerbi_station_table)
    .limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "sncf_lakehouse"
schema = "transport"

df_traffic = spark.table(
    f"{catalog}.{schema}.gold_station_traffic"
)

df_stops_coordinates = (
    spark.table(
        f"{catalog}.{schema}.bronze_stops"
    )
    .select(
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon"
    )
    .dropDuplicates(["stop_id"])
)

# COMMAND ----------


df_powerbi_station = (
    df_traffic.alias("traffic")
    .join(
        df_dim_stations.alias("stations"),
        F.regexp_extract(
            F.col("traffic.stop_id"),
            r"(\d{8})$",
            1
        ) == F.col("stations.code_uic").cast("string"),
        "left"
    )
    .join(
        df_stops_coordinates.alias("stops"),
        F.col("traffic.stop_id") == F.col("stops.stop_id"),
        "left"
    )
    .select(
        F.col("traffic.stop_id").alias("stop_id"),
        F.coalesce(
            F.col("traffic.stop_name"),
            F.col("stops.stop_name")
        ).alias("stop_name"),
        F.col("traffic.number_of_trips").alias("number_of_trips"),
        F.col("traffic.number_of_routes").alias("number_of_routes"),
        F.coalesce(
            F.col("stations.station_name"),
            F.col("traffic.stop_name"),
            F.col("stops.stop_name")
        ).alias("station_name"),
       F.coalesce(
            F.col("stations.city"),
            F.lit("Unknown / Outside France")
        ).alias("city"),

        F.coalesce(
            F.col("stations.department"),
            F.lit("Not available")
        ).alias("department"),
        F.coalesce(
            F.col("stations.latitude").cast("double"),
            F.col("stops.stop_lat").cast("double")
        ).alias("latitude"),
        F.coalesce(
            F.col("stations.longitude").cast("double"),
            F.col("stops.stop_lon").cast("double")
        ).alias("longitude")
    )
)

# COMMAND ----------

# (
#     df_powerbi_station.write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(
#         "sncf_lakehouse.transport.gold_powerbi_station"
#     )
# )

(
    df_powerbi_station
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "sncf_lakehouse.transport.gold_powerbi_station"
    )
)

# COMMAND ----------

display(
    df_powerbi_station
    .filter(
        F.col("city").isNull() |
        F.col("department").isNull()
    )
    .select(
        "stop_id",
        "stop_name",
        "station_name",
        "city",
        "department",
        "latitude",
        "longitude"
    )
    .limit(50)
)