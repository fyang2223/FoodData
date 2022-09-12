import sys
import psycopg2
import boto3
import base64
import json
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "RedshiftClusterInfo"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    else:
        secret = {}
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret

s = json.loads(get_secret())
host = s.get('host')
host = 'redshift-cluster-public-subnet.cqmyxv38tmti.us-east-1.redshift.amazonaws.com'
dbname = s.get('dbname')
port = s.get('port')
user = s.get('user')
password = s.get('password')

conn = psycopg2.connect(dbname=dbname, user=user, password=password, port=port, host=host)
cur = conn.cursor()

DROP_TABLE_FOOD = "DROP TABLE IF EXISTS food;"
DROP_TABLE_NUTRIENTS = "DROP TABLE IF EXISTS nutrients;"

CREATE_TABLE_FOOD = """
    CREATE TABLE IF NOT EXISTS food (
        fdc_id INTEGER,
        description TEXT,
        publicationDate TEXT,
        brandOwner TEXT,
        brandedFoodCategory TEXT,
        ingredients VARCHAR(2048),
        marketCountry TEXT,
        servingSize DOUBLE PRECISION,
        servingSizeUnit TEXT
    );
    """ 

CREATE_TABLE_NUTRIENTS = """
    CREATE TABLE IF NOT EXISTS nutrients (
        fdc_id INTEGER,
        name TEXT,
        nutrient_id INTEGER,
        nutrient_number TEXT,
        amount DOUBLE PRECISION,
        unitName TEXT
    );
""" 

COPYDATA_FOOD = """
    COPY food FROM 's3://usdacsvs/FOODS/'
    iam_role 'arn:aws:iam::082206757367:role/service-role/AmazonRedshift-CommandsAccessRole-20220908T135336'
    CSV 
    IGNOREHEADER 1
    region 'us-east-1';
"""

COPYDATA_NUTRIENTS = """
    COPY nutrients FROM 's3://usdacsvs/NUTRIENTS/'
    iam_role 'arn:aws:iam::082206757367:role/service-role/AmazonRedshift-CommandsAccessRole-20220908T135336'
    CSV 
    IGNOREHEADER 1
    region 'us-east-1';
"""


try:
    cur.execute(DROP_TABLE_FOOD)
    cur.execute(CREATE_TABLE_FOOD)
    cur.execute(COPYDATA_FOOD)
    
    cur.execute(DROP_TABLE_NUTRIENTS)
    cur.execute(CREATE_TABLE_NUTRIENTS)
    cur.execute(COPYDATA_NUTRIENTS)
    
    
    conn.commit()
except Exception as e:
    print(e)
    
    
cur.close()
conn.close()