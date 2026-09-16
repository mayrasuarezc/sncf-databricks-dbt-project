# Databricks notebook source
#Silver table 

from pyspark.sql import functions as F

catalog = "sncf_lakehouse"
schema = "transport"

silver_table = (
    f"{catalog}.{schema}.silver_trip_stops"
)

df_silver = spark.table(silver_table)

display(df_silver.limit(10))


# COMMAND ----------

total_rows = df_silver.count()

print(f"Total de filas Silver: {total_rows:,}")

# COMMAND ----------

columns_to_check = [
    "trip_id",
    "route_id",
    "stop_id",
    "stop_sequence",
    "stop_name"
]

null_counts = df_silver.select([
    F.sum(
        F.when(
            F.col(column_name).isNull(),
            1
        ).otherwise(0)
    ).alias(column_name)
    for column_name in columns_to_check
])

display(null_counts)

# COMMAND ----------

null_percentage = df_silver.select([
    (
        F.sum(
            F.when(
                F.col(column_name).isNull(),
                1
            ).otherwise(0)
        ) / F.lit(total_rows) * 100
    ).alias(f"{column_name}_null_pct")
    for column_name in columns_to_check
])

display(null_percentage)


# COMMAND ----------

df_invalid_keys = df_silver.filter(
    F.col("trip_id").isNull()
    | F.col("route_id").isNull()
    | F.col("stop_id").isNull()
    | F.col("stop_sequence").isNull()
)

display(df_invalid_keys.limit(20))

# COMMAND ----------

duplicate_groups = (
    df_silver
    .groupBy("trip_id", "stop_sequence")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_groups.limit(20))

# COMMAND ----------

duplicate_group_count = duplicate_groups.count()

print(
    f"Grupos duplicados: {duplicate_group_count:,}"
)

# COMMAND ----------

invalid_stop_sequence = df_silver.filter(
    F.col("stop_sequence") < 0
)

print(
    "Filas con stop_sequence inválido:",
    invalid_stop_sequence.count()
)

# COMMAND ----------

invalid_coordinates = df_silver.filter(
    (F.col("stop_lat").cast("double") < -90)
    | (F.col("stop_lat").cast("double") > 90)
    | (F.col("stop_lon").cast("double") < -180)
    | (F.col("stop_lon").cast("double") > 180)
)

display(invalid_coordinates.limit(20))

# COMMAND ----------

df_stops = spark.table(
    f"{catalog}.{schema}.bronze_stops"
).select("stop_id").dropDuplicates()

# COMMAND ----------

orphan_stops = (
    df_silver
    .select("stop_id")
    .dropDuplicates()
    .join(
        df_stops,
        on="stop_id",
        how="left_anti"
    )
)

display(orphan_stops)

# COMMAND ----------

orphan_stop_count = orphan_stops.count()

print(
    f"Stop IDs sin correspondencia: {orphan_stop_count:,}"
)

# COMMAND ----------

quality_metrics = [
    (
        "silver_trip_stops",
        "total_rows",
        total_rows
    ),
    (
        "silver_trip_stops",
        "duplicate_groups",
        duplicate_group_count
    ),
    (
        "silver_trip_stops",
        "orphan_stop_ids",
        orphan_stop_count
    ),
    (
        "silver_trip_stops",
        "invalid_stop_sequence",
        invalid_stop_sequence.count()
    )
]

df_quality_metrics = spark.createDataFrame(
    quality_metrics,
    [
        "dataset",
        "metric_name",
        "metric_value"
    ]
)

display(df_quality_metrics)

# COMMAND ----------

critical_failures = {
    "empty_dataset": total_rows == 0,
    "duplicate_groups": duplicate_group_count > 0,
    "invalid_stop_sequence": invalid_stop_sequence.count() > 0
}

display(
    spark.createDataFrame(
        [
            (check_name, failed)
            for check_name, failed
            in critical_failures.items()
        ],
        ["check_name", "failed"]
    )
)

# COMMAND ----------

if critical_failures["empty_dataset"]:
    raise ValueError(
        "La tabla Silver está vacía"
    )

if critical_failures["duplicate_groups"]:
    raise ValueError(
        "Existen duplicados por trip_id y stop_sequence"
    )

if critical_failures["invalid_stop_sequence"]:
    raise ValueError(
        "Existen stop_sequence inválidos"
    )

# COMMAND ----------

audit_table = (
    f"{catalog}.{schema}.audit_quality"
)

(
    df_quality_metrics
    .withColumn(
        "execution_timestamp",
        F.current_timestamp()
    )
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(audit_table)
)

# COMMAND ----------

display(
    spark.table(audit_table)
    .orderBy(
        F.col("execution_timestamp").desc()
    )
)

# COMMAND ----------

quarantine_table = (
    f"{catalog}.{schema}.quarantine_trip_stops"
)

(
    df_invalid_keys
    .withColumn(
        "_quarantine_reason",
        F.lit("Missing mandatory key")
    )
    .withColumn(
        "_quarantine_timestamp",
        F.current_timestamp()
    )
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(quarantine_table)
)

# COMMAND ----------

df_invalid_sequence = df_silver.filter(
    F.col("stop_sequence") < 0
)

display(
    df_invalid_sequence
    .groupBy("stop_sequence")
    .count()
    .orderBy("stop_sequence")
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sequence_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
)

df_sequence_check = (
    df_silver
    .withColumn(
        "previous_stop_sequence",
        F.lag("stop_sequence").over(sequence_window)
    )
)

# COMMAND ----------

df_sequence_errors = df_sequence_check.filter(
    F.col("previous_stop_sequence").isNotNull()
    & (
        F.col("stop_sequence")
        <= F.col("previous_stop_sequence")
    )
)

display(
    df_sequence_errors.select(
        "trip_id",
        "stop_id",
        "stop_sequence",
        "previous_stop_sequence"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_invalid_sequence = df_silver.filter(
    F.col("stop_sequence").isNull()
    | (F.col("stop_sequence") < 0)
)

invalid_sequence_count = df_invalid_sequence.count()

sequence_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
)

df_sequence_check = (
    df_silver
    .withColumn(
        "previous_stop_sequence",
        F.lag("stop_sequence").over(sequence_window)
    )
)

df_sequence_errors = df_sequence_check.filter(
    F.col("previous_stop_sequence").isNotNull()
    & (
        F.col("stop_sequence")
        <= F.col("previous_stop_sequence")
    )
)

sequence_order_error_count = df_sequence_errors.count()

print(
    f"Valores nulos o negativos: {invalid_sequence_count:,}"
)

print(
    f"Errores de orden: {sequence_order_error_count:,}"
)

# COMMAND ----------

quality_metrics_extended = [
    (
        "silver_trip_stops",
        "total_rows",
        total_rows
    ),
    (
        "silver_trip_stops",
        "duplicate_groups",
        duplicate_group_count
    ),
    (
        "silver_trip_stops",
        "orphan_stop_ids",
        orphan_stop_count
    ),
    (
        "silver_trip_stops",
        "invalid_stop_sequence",
        invalid_sequence_count
    ),
    (
        "silver_trip_stops",
        "sequence_order_errors",
        sequence_order_error_count
    )
]

df_quality_metrics_extended = spark.createDataFrame(
    quality_metrics_extended,
    [
        "dataset",
        "metric_name",
        "metric_value"
    ]
)

display(df_quality_metrics_extended)