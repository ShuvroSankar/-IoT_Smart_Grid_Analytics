import os
import time
import random
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd

# Initialize BigQuery Client
project_id = os.getenv("GCP_PROJECT_ID")
client = bigquery.Client(project=project_id)

dataset_id = "industrial_energy_analytics"
# Standardized targets - if the typo table exists, we use it, otherwise target standard
machines_table = f"{project_id}.{dataset_id}.dim_machines"
date_table = f"{project_id}.{dataset_id}.dim_date"
fact_table = f"{project_id}.{dataset_id}.fact_energy_consumption"

# BQ Load Config for free-tier batch processing
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

def seed_dimension_tables_free_tier():
    print("⏳ Seeding dimension tables using Free-Tier Batch Loading...")
    
    # 1. Seed Machines
    try:
        machines_data = [
            {"machine_id": "MACH_01", "machine_name": "Primary CNC Milling Axis", "machine_type": "Milling", "factory_floor": "Line Alpha", "installation_date": "2021-03-12"},
            {"machine_id": "MACH_02", "machine_name": "High-Pressure Injection Press", "machine_type": "Molding", "factory_floor": "Line Alpha", "installation_date": "2021-08-19"},
            {"machine_id": "MACH_03", "machine_name": "Heavy Metal Hydraulic Stamper", "machine_type": "Stamping", "factory_floor": "Line Beta", "installation_date": "2022-01-05"},
            {"machine_id": "MACH_04", "machine_name": "Robotic Assembly Arm 4X", "machine_type": "Assembly", "factory_floor": "Line Beta", "installation_date": "2022-06-22"},
            {"machine_id": "MACH_05", "machine_name": "Main HVAC Climate Chiller", "machine_type": "Facilities", "factory_floor": "Utility Bay", "installation_date": "2020-11-15"}
        ]
        df = pd.DataFrame(machines_data)
        df["installation_date"] = pd.to_datetime(df["installation_date"]).dt.date
        
        # Using load_table_from_dataframe is 100% FREE on Sandbox!
        # Try writing to standard dim_machines first, fallback to dm_machines if 404 hits
        try:
            client.load_table_from_dataframe(df, machines_table, job_config=job_config).result()
        except Exception:
            client.load_table_from_dataframe(df, f"{project_id}.{dataset_id}.dm_machines", job_config=job_config).result()
        print("✅ dim_machines seeded successfully via Free Batch API.")
    except Exception as e:
        print(f"ℹ️ Machine seeding skipped: {e}")

    # 2. Seed Calendar
    try:
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2026, 12, 31)
        delta = (end_date - start_date).days + 1
        
        date_records = []
        for x in range(delta):
            d = start_date + timedelta(days=x)
            date_records.append({
                "date_key": int(d.strftime("%Y%m%d")),
                "full_date": d.strftime("%Y-%m-%d"),
                "day_of_week": int(d.strftime("%w")) + 1,
                "day_name": d.strftime("%A"),
                "month_name": d.strftime("%B"),
                "quarter": (d.month - 1) // 3 + 1,
                "year": d.year,
                "is_weekend": 1 if d.strftime("%A") in ["Saturday", "Sunday"] else 0
            })
        df_date = pd.DataFrame(date_records)
        df_date["full_date"] = pd.to_datetime(df_date["full_date"]).dt.date
        client.load_table_from_dataframe(df_date, date_table, job_config=job_config).result()
        print("✅ dim_date timeline seeded successfully via Free Batch API.")
    except Exception as e:
        print(f"ℹ️ Date seeding skipped: {e}")

def run_micro_batch_pipeline():
    print("⚡ Starting Free-Tier Micro-Batch Ingestion Engine...")
    machines = ["MACH_01", "MACH_02", "MACH_03", "MACH_04", "MACH_05"]
    
    while True:
        now = datetime.utcnow()
        date_key = int(now.strftime("%Y%m%d"))
        batch_payload = []
        
        for m_id in machines:
            v_base, c_base, t_base = 230.0, 12.0, 72.0
            voltage = round(v_base + random.uniform(-5.0, 5.0), 2)
            current = round(c_base + random.uniform(-2.0, 2.0), 2)
            temp = round(t_base + random.uniform(-3.0, 6.0), 2)
            
            is_anomaly = 0
            if random.random() < 0.05:
                is_anomaly = 1
                voltage = round(voltage + random.uniform(40.0, 70.0), 2)
                temp = round(temp + random.uniform(20.0, 35.0), 2)
            
            power_factor = round(random.uniform(0.82, 0.95), 3)
            active_power_kw = round((voltage * current * power_factor) / 1000, 4)
            energy_consumed_kwh = round(active_power_kw * (10 / 3600), 5)
            tariff_rate = 0.15 if 8 <= now.hour <= 20 else 0.08
            cost_usd = round(energy_consumed_kwh * tariff_rate, 5)
            
            batch_payload.append({
                "timestamp": now,
                "date_key": date_key,
                "machine_id": m_id,
                "voltage_v": voltage,
                "current_a": current,
                "active_power_kw": active_power_kw,
                "power_factor": power_factor,
                "temperature_c": temp,
                "energy_consumed_kwh": energy_consumed_kwh,
                "electricity_cost_usd": cost_usd,
                "is_anomaly": is_anomaly
            })
            
        try:
            # Convert micro-batch to DataFrame and upload as a FREE load job
            df_batch = pd.DataFrame(batch_payload)
            client.load_table_from_dataframe(df_batch, fact_table, job_config=job_config).result()
            print(f"📦 [{datetime.now().strftime('%H:%M:%S')}] Micro-batch uploaded successfully (5 rows) via Free Load Job API.")
        except Exception as e:
            print(f"❌ Batch upload failed: {e}")
            
        time.sleep(10) # Process batches every 10 seconds

if __name__ == "__main__":
    seed_dimension_tables_free_tier()
    run_micro_batch_pipeline()

