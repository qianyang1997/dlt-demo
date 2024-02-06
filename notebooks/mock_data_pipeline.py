# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

# COMMAND ----------

# MAGIC %md
# MAGIC Create or refresh raw data input. DLT's auto load functionality reads all files from a directory in the initial load, and reads net new files in all the subsequent runs.

# COMMAND ----------

@dlt.table(
    name="patient",
    path="/demo/raw/patient/"
)
def patient():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/demo/input/patient/")
    )


@dlt.table(
    name="appointment",
    path="/demo/raw/appointment/"
)
def appointment():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/demo/input/appointment/")
    )


@dlt.table(
    name="encounter",
    path="/demo/raw/encounter/"
)
def encounter():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/demo/input/encounter/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Create updated snapshot of raw files. If rows are received out of order, the CDC layer can still detect the latest status of a row. Since source data is append-only, the CDC views are only updated with net new information.

# COMMAND ----------

dlt.create_streaming_table(
    name="patient_cdc",
    path="/demo/cdc/patient/"
)
dlt.create_streaming_table(
    name="appointment_cdc",
    path="/demo/cdc/appointment/"
)
dlt.create_streaming_table(
    name="encounter_cdc",
    path="/demo/cdc/encounter/"
)


dlt.apply_changes(
    target="patient_cdc",
    source="patient",
    keys=["patient_id"],
    sequence_by=col("update_date"),
    ignore_null_updates=True,
    apply_as_deletes=expr("delete_date IS NOT NULL"),
    stored_as_scd_type=2,
)


dlt.apply_changes(
    target="appointment_cdc",
    source="appointment",
    keys=["appointment_id"],
    sequence_by=col("update_date"),
    ignore_null_updates=True,
    apply_as_deletes=expr("delete_date IS NOT NULL"),
    stored_as_scd_type=2,
)


dlt.apply_changes(
    target="encounter_cdc",
    source="encounter",
    keys=["encounter_id"],
    sequence_by=col("update_date"),
    ignore_null_updates=True,
    apply_as_deletes=expr("delete_date IS NOT NULL"),
    stored_as_scd_type=2,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Materialized view to show latest snapshot of appointments and corresponding encounter records for each patient.

# COMMAND ----------

@dlt.view
def type_of_visit_by_practice():
    patient = dlt.read("patient_cdc")
    appt = dlt.read("appointment_cdc")
    encounter = dlt.read("encounter_cdc")
    # df = (
    #     appt
    #     .join(encounter, on="appointment_id", how="inner")
    # )
    return appt
