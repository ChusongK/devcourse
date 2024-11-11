from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp
import json
import requests

import pandas as pd
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_data():
    # API 호출
    url = "https://restcountries.com/v3/all"
    response = requests.get(url)
    data = response.json()  # API 응답을 JSON으로 변환

    # 데이터 가공
    countries_data = []
    for country in data:
        name = country['name']['official']
        population = country['population']
        area = country['area']

        countries_data.append([name,
            population,
            area
        ])

    return countries_data


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
            name VARCHAR(255),
            population INT,
            area FLOAT
);""")

        for r in records:
            # 데이터를 삽입하는 SQL 쿼리
            sql = f"INSERT INTO {schema}.{table} (name, population, area) VALUES (%s, %s, %s);"
            print(sql)
            cur.execute(sql, (r[0], r[1], r[2]))            
        cur.execute("COMMIT;")   # cur.execute("END;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'assignment_v1',
    start_date = datetime(2024,11,8),
    catchup=False,
    tags=['assignment'],
    schedule = '30 6 * * 6'
) as dag:

    results = get_data()
    load("rnjscksthd2", "countries_info", results)