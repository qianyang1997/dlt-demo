# Databricks notebook source
# MAGIC %md
# MAGIC Initialize storage directories.

# COMMAND ----------

# MAGIC %%bash
# MAGIC rm -rf /dbfs/demo
# MAGIC mkdir -p /dbfs/demo/input/{patient,appointment,encounter}
# MAGIC mkdir -p /dbfs/demo/output/{patient,appointment,encounter}

# COMMAND ----------

# MAGIC %md
# MAGIC Create initial data load for patient, appointment, and encounter.

# COMMAND ----------

import pandas as pd


patient_day1 = pd.DataFrame(
    {
        "patient_id": [101, 102, 103, 104],
        "name": ["Isador Brenan", "Kate Pearl", "Emily Zoe", "Jenny Taylor"],
        "zip": ["10001", "10002", "10002", "100003"],
        "create_date": ["2024-02-01", "2024-02-01", "2024-02-01", "2024-02-02"],
        "update_date": ["2024-02-02", "2024-02-02", "2024-02-02", "2024-02-02"],
        "delete_date": [None, None, None, None],
    }
)

appointment_day1 = pd.DataFrame(
    {
        "appointment_id": [10001, 10002, 10003, 10004],
        "patient_id": [101, 101, 105, 102],
        "appointment_status": ["created", "confirmed", "completed", "completed"],
        "practice": ["Qiana Yang M.D.", "Qiana Yang M.D.", "Sophie Yang M.D.", "Sophie Yang M.D."],
        "create_date": ["2024-02-01", "2024-02-01", "2024-02-01", "2024-02-01"],
        "update_date": ["2024-02-01", "2024-02-02", "2024-02-02", "2024-02-02"],
        "delete_date": [None, None, None, None],
    }
)

encounter_day1 = pd.DataFrame(
    {
        "encounter_id": [1, 2, 3, 4],
        "patient_id": [105, 102, 102, 102],
        "appointment_id": [10003, 10000, None, None],
        "visit_reason": ["follow up", "new patient", "walk in", "follow up"],
        "create_date": ["2024-02-02", "2024-01-01", "2024-01-01", "2024-02-01"],
        "update_date": ["2024-02-02", "2024-02-02", "2024-02-02", "2024-02-01"],
        "delete_date": [None, None, None, None]
    }
)

patient_day1.to_csv('/dbfs/demo/input/patient/patient_day1.csv', index=False)
appointment_day1.to_csv('/dbfs/demo/input/appointment/appointment_day1.csv', index=False)
encounter_day1.to_csv('/dbfs/demo/input/encounter/encounter_day1.csv', index=False)
