import streamlit as st
from sqlalchemy import *
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy.orm import sessionmaker

# Press the green button in the gutter to run the script.
AccessKeyId = "ASIA6I3PWQK3AYXVGWHN"
SecretAccessKey = "j0mK2obRE981yenPDS3d8NAOErx4aaoGcCxESJb6"
SessionToken = "IQoJb3JpZ2luX2VjEDkaCXVzLWVhc3QtMSJIMEYCIQDdxaftWemq04P9PgPMa3B0hoPerS/BC72oeyBjpQUWUAIhAPr4H/mZ1kyDZzImQgwz7xU4+Rvc0+P7KPSLGhYzpeqWKvQBCKL//////////wEQABoMOTgxMDk3NDE1MzUwIgznqp3sFgfbGUn+EegqyAHCDK9gkQwSq4/n73IGEHhnXKgezHVoGEp3oSKHUQJUJ21MdggazE6uMILFkJeE7xHTuZqGWRFd1CHFo0rPLaX7UkhwDsjAsvgqqgxZi0KrS2HzulfpUMRpPLUkuIXiRssd2a+ay10uyN8elEnvleLueRzTcu+NLBhoLhCXy5hgLVhnpsfwb1+/8MRhWxIUE8RUzihnN9Ny21ZXEMsmuvqeL/oD3NPJDuR8dV7Kl3sQaQyf61ORe/y1NiQz2ayvzcTHCLI1mXpFODDKgZeRBjqXAUjjrh7K3pPL/iWbbXhKuEN/tQ2bGNcDL0722cz49vLf7Rn6f9Mfh6LPA3+pY/8tBsHDa7dijk1TPntH1SESPx+oyPTS6Vq5tTHfV11wZXVd2Co8iSY+G77Nv6wqo7Mo/mRoxa3LF+CBEARu6rIv7Fc6dD1qFxYcgqwcUJvcleEFIxUsHxHS+xsdUxFivpGjfHDd34m4n3Q="
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

def purge_rds(engine):
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
    session.query(table).delete()
    session.commit()
    
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
    
if st.button('Vider RDS'):
    conn_db = connect_db()
    data_rds = purge_rds(conn_db)
    st.text(data_rds)
    conn_db = connect_db()
    data_rds = get_rds(conn_db)
    st.text(data_rds)
