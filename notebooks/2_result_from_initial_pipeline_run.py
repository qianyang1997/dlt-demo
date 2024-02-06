# Databricks notebook source
# MAGIC %md
# MAGIC Examine results from initial data dump in raw zone.

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
# MAGIC Examine results from initial data dump in CDC zone.

# COMMAND ----------

patient_cdc = spark.read.format("delta").load("/demo/cdc/patient")
appointment_cdc = spark.read.format("delta").load("/demo/cdc/appointment")
encounter_cdc = spark.read.format("delta").load("/demo/cdc/encounter")

# COMMAND ----------

patient_cdc.display()

# COMMAND ----------

appointment_cdc.display()

# COMMAND ----------

encounter_cdc.display()

# COMMAND ----------

patient.toPandas().to_csv("/dbfs/demo/output/patient/raw__patient_day1.csv")
appointment.toPandas().to_csv("/dbfs/demo/output/appointment/raw__appointment_day1.csv")
encounter.toPandas().to_csv("/dbfs/demo/output/encounter/raw__encounter_day1.csv")

patient_cdc.toPandas().to_csv("/dbfs/demo/output/patient/cdc__patient_day1.csv")
appointment_cdc.toPandas().to_csv("/dbfs/demo/output/appointment/cdc__appointment_day1.csv")
encounter_cdc.toPandas().to_csv("/dbfs/demo/output/encounter/cdc__encounter_day1.csv")
