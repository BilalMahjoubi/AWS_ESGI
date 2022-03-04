import streamlit as st
from sqlalchemy import *
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy.orm import sessionmaker

# Press the green button in the gutter to run the script.
AccessKeyId = "ASIA6I3PWQK3EOZ6TKHJ"
SecretAccessKey = "WakAXHxCNfaPEVhUnMYcOEb30MW+Pi6gtDT+u1pb"
SessionToken = "IQoJb3JpZ2luX2VjEPf//////////wEaCXVzLWVhc3QtMSJHMEUCIHYQistRzpWd4M16BvJMH3ROoCYH9KiSNzjxuP+ZH2aMAiEA45sXB/ilbR0b7cC/XF64LwOuZsN/DGROO9+qAU0iMOIq6wEIXxAAGgw5ODEwOTc0MTUzNTAiDPws8Q4aOjP7Ygq1ISrIAeZERuHY5oQHcI7HsSEzHpfyaKwafxsmmDyxpka1a5CyLlUaABMIQ6ukTQmS3qlv5J8soru3n6MsWs7/EkeM7cvn4G1Q9rM9jEbp4In/WNsJjksvbBfcYK6WIch2OO8vpLVKbQWMvxeAzqe4/2mmAuslA7hkkqwRL7MDjq32ghutHq/kz/VqjuoklH6Mq2+0yjhKoBdIJTZLro7wBPa6loA8eRBLzanSE9CyE+fp16uhJXV8kZBEOeRgLFmJHjBb2uahqqpbBINXMOO4iJEGOpgB2F9YHttsBf9rWpYRvfsU3DOkXHC5NSU4wgLt9YwP/UKH8dt/SfYxGa7N4YBSq7nsjLL621Iqijg5ySuz/S5mDqae7CT56RGzRa+aAjpeL2+SN5wkm2WleNqOJUTrl4lPhAUHrVS+wT6xXAPgwJ0RE2dIbgrQM9Q2eoDBd0228i0YMQdNCBAVYkb82QkMPxuJFjqGrm0S93U="
bucket_name = "projetbucketaws"
filename = "test.csv"


def connect_db():
    engine = create_engine("mysql://admin:bilamcod@test-db.cquuurrihevr.eu-west-3.rds.amazonaws.com:3306/testdb")
    return engine


def create_client():
    client = boto3.client('s3',
                          aws_access_key_id=AccessKeyId,
                          aws_secret_access_key=SecretAccessKey,
                          aws_session_token=SessionToken
                          )
    return client


def local_to_s3(client, path):
    df = pd.read_csv(path)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    response = client.put_object(Bucket='projetbucketaws', Body=csv_buf.getvalue(), Key='test.csv')
    return response['ResponseMetadata']['HTTPStatusCode']


def local_to_rds(engine, path):
    metadata = MetaData()
    metadata.bind = engine
    Session = sessionmaker(bind=engine)
    session = Session()
    df = pd.read_csv(path)
    df.to_sql(con=engine, name='matiere', if_exists='append', index=False)
    session.commit()
    return "ok"


def s3_to_local(bucket_name, local_path, name_object, client):
    client.download_file(bucket_name, name_object, local_path)
    print("dl ici" + local_path)


def s3_to_rds(engine, client, bucket_name, local_path, name_object):
    s3_to_local(bucket_name, local_path, name_object, client)
    local_to_rds(engine, local_path)


def get_rds(engine):
    metadata = MetaData()
    metadata.bind = engine
    Session = sessionmaker(bind=engine)
    session = Session()
    table = Table(
        'matiere',
        metadata,
        autoload=True,
        autoload_with=engine
    )
    matiere = session.query(table).all()
    return matiere

st.title('Projet AWS')

if st.button('Charger dans RDS'):
    conn_db = connect_db()
    local_to_rds(conn_db, "test1.csv")
    data_rds = get_rds(conn_db)
    st.text(data_rds)

if st.button('Charger dans S3'):
    client = create_client()
    aa = local_to_s3(client, "test1.csv")
    if(aa == 200):
        st.text("Chargement rÃ©ussie")
    else:
        st.text("Erreur")

if st.button('Transfere S3 vers RDS'):
    client = create_client()
    conn_db = connect_db()
    s3_to_rds(conn_db, client, "projetbucketaws", "./testS3toRDS.csv", "test.csv")
    data_rds = get_rds(conn_db)
    st.text(data_rds)

if st.button('Get RDS'):
    conn_db = connect_db()
    data_rds = get_rds(conn_db)
    st.text(data_rds)
