# Databricks notebook source
catalog = "sncf_lakehouse"
schema = "transport"
volume = "raw"

base_path = f"/Volumes/{catalog}/{schema}/{volume}"

print(base_path)

# COMMAND ----------

gtfs_path = f"{base_path}/gtfs"
reference_path = f"{base_path}/reference"
realtime_path = f"{base_path}/realtime"

print(gtfs_path)
print(reference_path)
print(realtime_path)

# COMMAND ----------

#Create the files inside the volume
dbutils.fs.mkdirs(gtfs_path)
dbutils.fs.mkdirs(reference_path)
dbutils.fs.mkdirs(realtime_path)

# COMMAND ----------

display(dbutils.fs.ls(base_path))

# COMMAND ----------

display(dbutils.fs.ls(gtfs_path))

# COMMAND ----------

display(dbutils.fs.ls(reference_path))

# COMMAND ----------

display(dbutils.fs.ls(realtime_path))

# COMMAND ----------

#Verify Errors in routes
for folder_path in [gtfs_path, reference_path, realtime_path]:
    print(f"\nContenido de: {folder_path}")
    for file_info in dbutils.fs.ls(folder_path):
        print(file_info.name, file_info.size)

# COMMAND ----------

df_routes = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .option("inferSchema", False)
    .load(f"{gtfs_path}/routes.txt")
)

display(df_routes.limit(10))

# COMMAND ----------

df_trips = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .option("inferSchema", False)
    .load(f"{gtfs_path}/trips.txt")
)

display(df_trips.limit(10))

# COMMAND ----------

df_stations = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .load(f"{reference_path}/liste-des-gares.csv")
)

display(df_stations.limit(10))