import pandas as pd

from config.db_config import MYSQL_CONFIG_A, MYSQL_CONFIG_B
from etl.extract import extract_mysql_data
from etl.load import load_to_bigquery
from etl.transform import (
    clean_patient_data, 
    clean_transactions, 
    clean_encounters, 
    clean_providers, 
    clean_departments
)

# Step 1: Extract from both hospitals
data_a = extract_mysql_data(MYSQL_CONFIG_A, 'A')
data_b = extract_mysql_data(MYSQL_CONFIG_B, 'B')

# Step 2: Transform and combine tables

# Patients
patients = pd.concat([data_a['patients'], data_b['patients']], ignore_index=True)
patients = clean_patient_data(patients)

# Transactions
transactions = pd.concat([data_a['transactions'], data_b['transactions']], ignore_index=True)
transactions = clean_transactions(transactions)

# Encounters
encounters = pd.concat([data_a['encounters'], data_b['encounters']], ignore_index=True)
encounters = clean_encounters(encounters)

# Providers
providers = pd.concat([data_a['providers'], data_b['providers']], ignore_index=True)
providers = clean_providers(providers)

# Departments
departments = pd.concat([data_a['departments'], data_b['departments']], ignore_index=True)
departments = clean_departments(departments)

# Step 3: Load into BigQuery
load_to_bigquery(patients, 'dim_patients')
load_to_bigquery(transactions, 'fact_transactions')
load_to_bigquery(encounters, 'fact_encounters')
load_to_bigquery(providers, 'dim_providers')
load_to_bigquery(departments, 'dim_departments')
