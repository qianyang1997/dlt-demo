# Databricks notebook source
import pandas as pd


patient_day2 = pd.DataFrame(
    {
        "patient_id": [104, 105],
        "name": ["Jenny Taylor", "John Doe"],
        "zip": ["100003", "100003"],
        "create_date": ["2024-02-02", "2024-02-03"],
        "update_date": ["2024-02-03", "2024-02-03"],
        "delete_date": ["2024-02-03", None],
    }
)

appointment_day2 = pd.DataFrame(
    {
        "appointment_id": [10001, 10002],
        "patient_id": [101, 102],
        "appointment_status": ["confirmed", "completed"],
        "practice": ["Qiana Yang M.D.", "Qiana Yang M.D."],
        "create_date": ["2024-02-01", "2024-02-01"],
        "update_date": ["2024-02-03", "2024-02-03"],
        "delete_date": [None, None],
    }
)

encounter_day2 = pd.DataFrame(
    {
        "encounter_id": [5],
        "patient_id": [102],
        "appointment_id": [10002],
        "visit_reason": ["follow up"],
        "create_date": ["2024-02-01"],
        "update_date": ["2024-02-03"],
        "delete_date": [None]
    }
)

patient_day2.to_csv('/dbfs/demo/input/patient/patient_day2.csv', index=False)
appointment_day2.to_csv('/dbfs/demo/input/appointment/appointment_day2.csv', index=False)
encounter_day2.to_csv('/dbfs/demo/input/encounter/encounter_day2.csv', index=False)
