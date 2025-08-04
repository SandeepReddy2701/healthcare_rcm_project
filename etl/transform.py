import pandas as pd

def clean_patient_data(df):
    # Fix date columns, fill missing data, remove duplicates
    df = df.copy()
    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')
    df['ModifiedDate'] = pd.to_datetime(df['ModifiedDate'], errors='coerce')
    df.drop_duplicates(subset='PatientID', inplace=True)
    return df

def clean_transactions(df):
    df = df.copy()
    df['PaidAmount'] = df['PaidAmount'].fillna(0)
    df['VisitDate'] = pd.to_datetime(df['VisitDate'], errors='coerce')
    df['PaidDate'] = pd.to_datetime(df['PaidDate'], errors='coerce')
    return df

def clean_encounters(df):
    df = df.copy()
    df['EncounterDate'] = pd.to_datetime(df['EncounterDate'], errors='coerce')
    df.drop_duplicates(inplace=True)
    return df

def clean_providers(df):
    df = df.copy()
    df.drop_duplicates(inplace=True)
    return df

def clean_departments(df):
    df = df.copy()
    df.drop_duplicates(inplace=True)
    return df
