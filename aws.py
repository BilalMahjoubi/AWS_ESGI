import streamlit as st
from sqlalchemy import *
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy.orm import sessionmaker

# Press the green button in the gutter to run the script.
AccessKeyId = "ASIA6I3PWQK3BZIVN4SG"
SecretAccessKey = "hoanJkgwBGRQ/tv2NkcoY3prja0FIgKEW0z8hzry"
SessionToken = "IQoJb3JpZ2luX2VjEN7//////////wEaCXVzLWVhc3QtMSJIMEYCIQCELyCrBD+/Iho0jQxGrzfS2sKCvJqe1vhcQx5NrUglOwIhAN8onKP5mFrS0IxAfA5kHic97vGdAciSbo2FP5qhe2c7KusBCEcQABoMOTgxMDk3NDE1MzUwIgxOdDl/PlE8vRl+r88qyAEIvyJO5G50/GGRmteV7fwSMZwFV1hPo+DquiuR4y5NTktq3jaSeEEZUm/+82ZkrI6NNZTlwVr4paDfwfDTQ4zSazmBrpgRpBSixajD94BARzv3z0uEWDdhqftUPpwbYrWtBC3B9qtGaJGjujgH8cj5ces7o6Ek06ttzb6C3JTHppcPyhxDYYu+vO1NR7s3mE+L+bd8hd4Rx4U1gLRjPj0ILEagbR54/nWUGjf8TOd5HKIGPOYVjpTxlrDS3Xu7JZTsg8M5E6trFDDwkIORBjqXAXNY6qClv9UX68GxSgEjONAlKY6b1dTV6X5Tj5AUfLoKzNMB3TP86tQ18V+CpP11h9YyXgHZMX7yEzAcqGcg/SjlGMu9gpqxnkdvLzkattdn7ECXgG4DFkDtLGFTyGRQ8vQIsBnVqnjy6xjviIjFhZpP+jp+b0BPqM7mdGhxeOD4fgbVv6Td8i6prCsv1MunoPzgq0LKgOA="
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