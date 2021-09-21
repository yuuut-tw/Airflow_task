# Step 1 => Import necessary packages
import datetime as dt
import pandas as pd
import re
import csv
import pymysql

# Backwards compatibility of pymysql to mysqldb
pymysql.install_as_MySQLdb()

# Importing MySQLdb now
import MySQLdb

# For Apache Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # using only PythonOperator this time


# Step 2 => Define functions for each operators

# A JSON string reader to .csv writer function.
def jsonToCsv(url, outputcsv):

    # Reads the JSON string into a pandas DataFrame object and simple data-cleansing
    df = pd.read_json(url)
    cols = ['作物代號', '作物名稱', '市場名稱', '溯源代號']
    df[cols] = df[cols].applymap(lambda x: re.sub(" ", "", x))
    
    # Convert the object to a .csv file.
    df.to_csv(outputcsv, header=False)

    return 'Read JSON and output CSV'


def csvToSql(file):

    # Attempt connection to a database
    try:
        dbconnect = MySQLdb.connect(
                host='localhost',
                user='root',
                passwd='root',
                db='airflow'
                )
    except:
        print('Can not connect')

    # Define a cursor iterator object to function and to drop the table if exists.
    cursor = dbconnect.cursor()
    cursor.execute("drop table if exists airflow_test")

    # Create table
    cursor.execute("""CREATE table IF NOT EXISTS airflow_test (ˋ號碼ˋ varchar(10) primary key, ˋ交易日期ˋ varchar(10), 
                                                               ˋ作物代號ˋ varchar(10), ˋ作物名稱ˋ varchar(20), ˋ市場代號ˋ varchar(20), ˋ市場名稱ˋ varchar(10),
                                                               ˋ交易金額_元ˋ varchar(20), ˋ交易量_公斤ˋ varchar(20), ˋ溯源代號ˋ varchar(10))""")

    # Open and read from the .csv file
    with open(f'./{file}', encoding='utf8') as f:

        # Assign the .csv data that will be iterated by the cursor.
        csv_data = csv.reader(f)

        # Insert data using SQL statements and Python
        for row in csv_data:
            cursor.execute(f'''INSERT INTO airflow_test (ˋ號碼ˋ, ˋ交易日期ˋ, ˋ作物代號ˋ, ˋ作物名稱ˋ, ˋ市場代號ˋ, ˋ市場名稱ˋ, ˋ交易金額_元ˋ, ˋ交易量_公斤ˋ, ˋ溯源代號ˋ) 
                               VALUES ("{row[0]}", "{row[1]}", "{row[2]}", "{row[3]}", "{row[4]}", "{row[5]}", "{row[6]}", "{row[7]}", "{row[8]}")''')
    # Commit the changes
    dbconnect.commit()

    # Close the connection
    cursor.close()

    # Confirm completion
    return 'Read CSV and insert values into DB'


# Step 3 => Define the DAG

# DAG's arguments
default_args = {
        'owner': 'yu',
        'start_date':dt.datetime(2021, 9, 17),
        'concurrency': 1,
        'retries': 0
        }

# DAG's operators
with DAG('parsing_govt_data',
        catchup=False,          # To skip any intervals we didn't run
        default_args=default_args,
        schedule_interval='* 1 * * * *', # 's m h d mo y'; set to run every minute.
        ) as dag:

    opr_json_to_csv = PythonOperator(
            task_id='json_to_csv',
            python_callable=jsonToCsv,
            op_kwargs={
                'url':'https://data.coa.gov.tw/Service/OpenData/FromM/TAPData.aspx',
                'outputcsv':'./test.csv'
                }
            )

    opr_csv_to_sql = PythonOperator(
            task_id='csv_to_sql',
            python_callable=csvToSql,
            op_kwargs={'file':'./test.csv'}
            )

# The actual workflow
opr_json_to_csv >> opr_csv_to_sql