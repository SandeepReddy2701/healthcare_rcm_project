import pandas as pd
from sqlalchemy import create_engine

def extract_mysql_data(config, hospital_prefix):
    conn_str = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(conn_str)

    patients = pd.read_sql("SELECT * FROM patients", engine)
    transactions = pd.read_sql("SELECT * FROM transactions", engine)
    encounters = pd.read_sql("SELECT * FROM encounters", engine)
    providers = pd.read_sql("SELECT * FROM providers", engine)
    departments = pd.read_sql("SELECT * FROM departments", engine)

    # Normalize patient columns
    if hospital_prefix == 'A':
        patients['unified_patient_id'] = 'A_' + patients['PatientID']
    else:
        patients['unified_patient_id'] = 'B_' + patients['ID']
        patients.rename(columns={
            'ID': 'PatientID',
            'F_Name': 'FirstName',
            'L_Name': 'LastName',
            'M_Name': 'MiddleName'
        }, inplace=True)

    return {
        'patients': patients,
        'transactions': transactions,
        'encounters': encounters,
        'providers': providers,
        'departments': departments
    }
