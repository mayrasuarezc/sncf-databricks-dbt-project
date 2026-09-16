# Databricks notebook source


from pyspark.sql import functions as F

catalog = "sncf_lakehouse"
schema = "transport"
volume = "raw"

base_path = f"/Volumes/{catalog}/{schema}/{volume}"

gtfs_path = f"{base_path}/gtfs"
reference_path = f"{base_path}/reference"
realtime_path = f"{base_path}/realtime"

print(gtfs_path)
print(reference_path)
print(realtime_path)

# COMMAND ----------

def read_gtfs_file(file_name):
    return (
        spark.read
        .format("csv")
        .option("header", True)
        .option("delimiter", ",")
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .load(f"{gtfs_path}/{file_name}")
    )

# COMMAND ----------

#Upload the gtfs as data frames 
df_agency = read_gtfs_file("agency.txt")
df_calendar_dates = read_gtfs_file("calendar_dates.txt")
df_feed_info = read_gtfs_file("feed_info.txt")
df_routes = read_gtfs_file("routes.txt")
df_stop_times = read_gtfs_file("stop_times.txt")
df_stops = read_gtfs_file("stops.txt")
df_transfers = read_gtfs_file("transfers.txt")
df_trips = read_gtfs_file("trips.txt")

# COMMAND ----------

display(df_routes.limit(10))
display(df_trips.limit(10))
display(df_stops.limit(10))
display(df_stop_times.limit(10))

# COMMAND ----------

#Add 2 columns of audit
def add_bronze_metadata(df, source_file):
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

# COMMAND ----------

#Save all the Bronze dfs in a dictionary along with the audit columns of metadata
bronze_tables = {
    "agency": add_bronze_metadata(df_agency, "agency.txt"),
    "calendar_dates": add_bronze_metadata(
        df_calendar_dates,
        "calendar_dates.txt"
    ),
    "feed_info": add_bronze_metadata(df_feed_info, "feed_info.txt"),
    "routes": add_bronze_metadata(df_routes, "routes.txt"),
    "stop_times": add_bronze_metadata(
        df_stop_times,
        "stop_times.txt"
    ),
    "stops": add_bronze_metadata(df_stops, "stops.txt"),
    "transfers": add_bronze_metadata(
        df_transfers,
        "transfers.txt"
    ),
    "trips": add_bronze_metadata(df_trips, "trips.txt")
}

# COMMAND ----------

#Save the delta bronze tables
for table_name, df in bronze_tables.items():
    full_table_name = f"{catalog}.{schema}.bronze_{table_name}"
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(full_table_name)
    )
    
    print(f"Tabla creada: {full_table_name}")

# COMMAND ----------

spark.sql(f"""
SHOW TABLES IN {catalog}.{schema}
""").show(truncate=False)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.bronze_routes")
)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.bronze_stop_times")
    .limit(20)
)

# COMMAND ----------

#Show how many rows has bronze table
for table_name in bronze_tables.keys():
    full_table_name = f"{catalog}.{schema}.bronze_{table_name}"
    count = spark.table(full_table_name).count()
    print(f"{full_table_name}: {count:,} filas")

# COMMAND ----------

#We add the reference source
stations_file_path = f"{reference_path}/liste-des-gares.csv"

df_stations = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .load(stations_file_path)
)

display(df_stations.limit(10))

# COMMAND ----------

stations_file_path = f"{reference_path}/liste-des-gares.csv"

df_stations_raw = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .load(stations_file_path)
)

display(df_stations_raw.limit(10))

# COMMAND ----------

print(df_stations_raw.columns)

# COMMAND ----------

station_columns = [
    "code_uic",
    "libelle",
    "fret",
    "voyageurs",
    "code_ligne",
    "rg_troncon",
    "pk",
    "commune",
    "departement",
    "idreseau",
    "idgaia",
    "x_l93",
    "y_l93",
    "x_wgs84",
    "y_wgs84",
    "c_geo",
    "geo_point",
    "geo_shape"
]

print(len(df_stations_raw.columns))
print(len(station_columns))

# COMMAND ----------

df_stations_clean = df_stations_raw.toDF(*station_columns)

print(df_stations_clean.columns)

# COMMAND ----------

display(df_stations_clean.limit(10))

# COMMAND ----------

df_stations_clean.printSchema()

# COMMAND ----------

#Add metadata to clean table
from pyspark.sql import functions as F

bronze_stations = (
    df_stations_clean
    .withColumn(
        "_source_file",
        F.lit("liste-des-gares.csv")
    )
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

#Save
stations_table = "sncf_lakehouse.transport.bronze_stations"

(
    bronze_stations.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(stations_table)
)

# COMMAND ----------

display(
    spark.table(stations_table).limit(10)
)

# COMMAND ----------

#See all the tables
spark.sql("""
SHOW TABLES IN sncf_lakehouse.transport
""").show(truncate=False)

# COMMAND ----------

#Test number of rows
bronze_table_names = [
    "bronze_agency",
    "bronze_calendar_dates",
    "bronze_feed_info",
    "bronze_routes",
    "bronze_stop_times",
    "bronze_stops",
    "bronze_transfers",
    "bronze_trips",
    "bronze_stations"
]

for table_name in bronze_table_names:
    full_table_name = (
        f"sncf_lakehouse.transport.{table_name}"
    )
    
    row_count = spark.table(full_table_name).count()
    
    print(f"{table_name}: {row_count:,} filas")

# COMMAND ----------

#Validate audit columns
df_bronze_routes = spark.table(
    "sncf_lakehouse.transport.bronze_routes"
)

display(
    df_bronze_routes.select(
        "_source_file",
        "_ingestion_timestamp"
    ).limit(5)
)

# COMMAND ----------

#Validate stations
df_bronze_stations = spark.table(
    "sncf_lakehouse.transport.bronze_stations"
)

display(
    df_bronze_stations.select(
        "code_uic",
        "libelle",
        "geo_point",
        "geo_shape",
        "_source_file",
        "_ingestion_timestamp"
    ).limit(10)
)

# COMMAND ----------

display(
    df_bronze_routes.filter(
        F.col("route_id").isNull()
    )
)

# COMMAND ----------

df_bronze_trips = spark.table(
    "sncf_lakehouse.transport.bronze_trips"
)

display(
    df_bronze_trips.filter(
        F.col("trip_id").isNull()
        | F.col("route_id").isNull()
    )
)

# COMMAND ----------

df_bronze_stop_times = spark.table(
    "sncf_lakehouse.transport.bronze_stop_times"
)

display(
    df_bronze_stop_times.filter(
        F.col("trip_id").isNull()
        | F.col("stop_id").isNull()
        | F.col("stop_sequence").isNull()
    ).limit(20)
)

# COMMAND ----------

#Summary View
summary_data = []

for table_name in bronze_table_names:
    full_table_name = (
        f"sncf_lakehouse.transport.{table_name}"
    )
    
    df = spark.table(full_table_name)
    
    summary_data.append((
        table_name,
        df.count(),
        len(df.columns)
    ))

df_bronze_summary = spark.createDataFrame(
    summary_data,
    [
        "table_name",
        "row_count",
        "column_count"
    ]
)

display(df_bronze_summary)

# COMMAND ----------

#Explain show the execution plan of Spark --- Learn scans, filters and shuffles
df_bronze_stop_times.explain("formatted")