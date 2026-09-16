# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

catalog = "sncf_lakehouse"
schema = "transport"

# Step 1: Simulate an incoming correction feed.
# Take 5 existing routes and pretend SNCF sent corrected long names for them,
# plus we'll sneak in one brand-new route_id that doesn't exist yet.
existing_routes = spark.table(f"{catalog}.{schema}.bronze_routes").limit(5)

updates_df = (
    existing_routes
    .withColumn("route_long_name", F.concat(F.col("route_long_name"), F.lit(" (Corrected)")))
)

# Add one fake new route to simulate an insert
new_route = spark.createDataFrame(
    [("NEW_ROUTE_999", "SNCF", "TGV999", "Paris - Marseille Express (New)", "2")],
    ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]
)

incoming_batch = updates_df.select(
    "route_id", "agency_id", "route_short_name", "route_long_name", "route_type"
).unionByName(new_route)

# Get the real table's schema, and add any missing columns as null automatically
real_schema = spark.table(f"{catalog}.{schema}.bronze_routes").schema

for field in real_schema:
    if field.name not in incoming_batch.columns:
        incoming_batch = incoming_batch.withColumn(field.name, F.lit(None).cast(field.dataType))

display(incoming_batch)

display(incoming_batch)

# COMMAND ----------

# Step 2: Wrap the real Delta table as a DeltaTable object.
# This is required to use merge/update/delete operations — a plain DataFrame can't do this.
routes_delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.bronze_routes")

(
    routes_delta_table.alias("target")
    .merge(
        incoming_batch.alias("source"),
        "target.route_id = source.route_id"
    )
    .whenMatchedUpdate(set={
        "route_long_name": "source.route_long_name"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

merge_result = (
    routes_delta_table.alias("target")
    .merge(
        incoming_batch.alias("source"),
        "target.route_id = source.route_id"
    )
    .whenMatchedUpdate(set={"route_long_name": "source.route_long_name"})
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.bronze_routes")
    .select("version", "timestamp", "operation", "operationMetrics")
    .limit(1)
)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.bronze_routes")
    .filter(F.col("route_id") == "NEW_ROUTE_999")
)

# COMMAND ----------

# Version story
display(
    spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.bronze_routes")
    .select("version", "timestamp", "operation")
)


# COMMAND ----------

# Query the table as it looked BEFORE your merge (adjust version number based on what you see above)
df_previous_version = spark.sql(f"""
SELECT route_id, route_long_name
FROM {catalog}.{schema}.bronze_routes VERSION AS OF 2
WHERE route_id = 'FR:Line::00F2577A-6A87-42E0-95F3-07351E4BC2F6:'
""")

display(df_previous_version)

# COMMAND ----------

df_current_version = spark.sql(f"""
SELECT route_id, route_long_name
FROM {catalog}.{schema}.bronze_routes
WHERE route_id = 'FR:Line::00F2577A-6A87-42E0-95F3-07351E4BC2F6:'
""")

display(df_current_version)

# COMMAND ----------

# Create a small new change to generate version 4
test_update = spark.sql(f"""
SELECT route_id, agency_id, route_short_name, 
       CONCAT(route_long_name, ' - TEST') as route_long_name,
       route_desc, route_type, route_url, route_color, route_text_color,
       _source_file, _ingestion_timestamp
FROM {catalog}.{schema}.bronze_routes
WHERE route_id = 'NEW_ROUTE_999'
""")

(
    routes_delta_table.alias("target")
    .merge(test_update.alias("source"), "target.route_id = source.route_id")
    .whenMatchedUpdate(set={"route_long_name": "source.route_long_name"})
    .execute()
)

# COMMAND ----------

spark.sql(f"""
SELECT route_long_name FROM {catalog}.{schema}.bronze_routes VERSION AS OF 3
WHERE route_id = 'NEW_ROUTE_999'
""").show(truncate=False)

spark.sql(f"""
SELECT route_long_name FROM {catalog}.{schema}.bronze_routes
WHERE route_id = 'NEW_ROUTE_999'
""").show(truncate=False)

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.bronze_routes")
    .select("numFiles", "sizeInBytes")
)

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{schema}.bronze_routes")

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{schema}.bronze_routes ZORDER BY (route_id)")

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.bronze_routes")
    .select("numFiles", "sizeInBytes")
)