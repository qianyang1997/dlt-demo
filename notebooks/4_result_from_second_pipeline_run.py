# Databricks notebook source
# MAGIC %md
# MAGIC Examine result of delta tables after ingestion of incremental load.

# COMMAND ----------

patient = spark.read.format("delta").load("/demo/raw/patient")
appointment = spark.read.format("delta").load("/demo/raw/appointment")
encounter = spark.read.format("delta").load("/demo/raw/encounter")

# COMMAND ----------

patient.display()

# COMMAND ----------

appointment.display()

# COMMAND ----------

encounter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Examine updated snapshots of tables in the CDC pipeline.

# COMMAND ----------

patient_cdc = spark.read.format("delta").load("/demo/cdc/patient")
appointment_cdc = spark.read.format("delta").load("/demo/cdc/appointment")
encounter_cdc = spark.read.format("delta").load("/demo/cdc/encounter")

# COMMAND ----------

patient_cdc.display()

# COMMAND ----------

appointment_cdc.display()

# COMMAND ----------

encounter.display()

# COMMAND ----------

patient.toPandas().to_csv("/dbfs/demo/output/patient/raw__patient_day2.csv")
appointment.toPandas().to_csv("/dbfs/demo/output/appointment/raw__appointment_day2.csv")
encounter.toPandas().to_csv("/dbfs/demo/output/encounter/raw__encounter_day2.csv")

patient_cdc.toPandas().to_csv("/dbfs/demo/output/patient/cdc__patient_day2.csv")
appointment_cdc.toPandas().to_csv("/dbfs/demo/output/appointment/cdc__appointment_day2.csv")
encounter_cdc.toPandas().to_csv("/dbfs/demo/output/encounter/cdc__encounter_day2.csv")
