import configparser
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, DateTime
import boto3
import csv

# Grab config
config = configparser.ConfigParser()
config.read('config.ini')

# Grab postgreSQL config
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
port = config.get('postgres', 'port')

# Database setup
engine = create_engine(f'postgresql://{username}:{password}@localhost:{port}/medical_db')
metadata = MetaData()

# Define tables
doctors = Table('doctors', metadata,
    Column('doctor_id', Integer, primary_key=True, autoincrement=True),
    Column('name', String),
    Column('speciality', String)
)

patients = Table('patients', metadata,
    Column('patient_id', Integer, primary_key=True, autoincrement=True),
    Column('first_name', String),
    Column('last_name', String),
    Column('age', Integer),
    Column('phone_number', String),
    Column('address', String),
    Column('city', String)
)

appointments = Table('appointments', metadata,
    Column('appointment_id', Integer, primary_key=True, autoincrement=True),
    Column('doctor_id', Integer, ForeignKey('doctors.doctor_id')),
    Column('patient_id', Integer, ForeignKey('patients.patient_id')),
    Column('reason', String),
    Column('appointment_time', DateTime)
)

metadata.create_all(engine)

# Load data from CSV files
doctor_data = pd.read_csv('doctor.csv')
patient_data = pd.read_csv('patient.csv')
appointment_data = pd.read_csv('appointment.csv')

doctor_data.to_sql('doctors', engine, if_exists='append', index=False)
patient_data.to_sql('patients', engine, if_exists='append', index=False)
appointment_data.to_sql('appointments', engine, if_exists='append', index=False)

# Data extraction
query = '''
SELECT doctors.name as doctor_name, doctors.speciality, patients.first_name, patients.last_name,
       patients.age, patients.phone_number, patients.address, patients.city,
       appointments.reason, appointments.appointment_time
FROM appointments
JOIN doctors ON appointments.doctor_id = doctors.doctor_id
JOIN patients ON appointments.patient_id = patients.patient_id
'''
results = engine.execute(query)
results_df = pd.DataFrame(results.fetchall(), columns=results.keys())

# Export to CSV
results_df.to_csv('joined_data.csv', index=False)

# Grab AWS config
access_key = config.get('aws', 'access_key')
secret_access_key = config.get('aws', 'secret_access_key')
bucket_name = config.get('aws', 'bucket_name')

# AWS S3 Integration
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
s3_bucket_name = bucket_name

s3.upload_file('joined_data.csv', s3_bucket_name, 'joined_data.csv')
