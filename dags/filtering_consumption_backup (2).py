from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.sensors.python_sensor import PythonSensor

from datetime import datetime,timedelta
import pandas as pd
import os

default_args = {
    'owner': 'chinhpn',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28, 11, 50),  # Start time for the DAG
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='filtering_customer_consumption_backup',
    default_args=default_args,
    schedule_interval=None,  # Set to None to manually trigger the DAG
    catchup=False,  # Prevent backfilling old data
    max_active_runs=1,  # Allow only one active DAG run at a time
) 

# Config
postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
execution_date_formatted = datetime.now().strftime('%Y%m%d')  # Format the execution date as YYYYMMDD
    # Use the PostgreSQL hook to connect to the database


#Task to check for raw data
def check_for_raw_data():
    raw_data_path = '/opt/airflow/dags/raw/consumption_yyyymmdd.csv'
    return os.path.exists(raw_data_path)


#Task to create table if not exist
Table_Names = [f"consumption_alcoholic_{execution_date_formatted}",
               f"consumption_cereals_bakery_{execution_date_formatted}",
               f"consumption_meats_poultry_{execution_date_formatted}"]

sql_create_table = ""
for table_name in Table_Names:
    sql_create_table += """
        DROP TABLE {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
            category VARCHAR(255),
            sub_category VARCHAR(255),
            aggregation_date DATE,
            millions_of_dollar INT,            
            pipeline_exc_datetime TIMESTAMP
        );
    """.format(table_name = table_name)


#Task to filter and load data
def filter_and_load_data(**kwargs):
    # Read the raw data
    raw_data_path = '/opt/airflow/dags/raw/consumption_yyyymmdd.csv'
    df = pd.read_csv(raw_data_path)

    # Convert 'Month' column to datetime
    df['Month'] = pd.to_datetime(df['Month'], errors='coerce', format='%d/%m/%Y').dt.strftime('%Y/%m/%d')
    
    # Format column names
    new_column_names = {
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Month': 'aggregation_date',
        'Millions of Dollars': 'millions_of_dollar',
    } 
    df = df.rename(columns=new_column_names)    
    
    # Get execution timestamp
    df['pipeline_exc_datetime'] = kwargs['ts']    

    # Create 3 separate df for 3 tables:
    df_alcoholic = df[df['category'] == 'Alcoholic beverages']
    df_cereals_bakery = df[df['category'] == 'Cereals and bakery products']
    df_meats_poultry = df[df['category'] == 'Meats and poultry']

    dataframes = [df_alcoholic, df_cereals_bakery, df_meats_poultry]
    
    # Insert data to postgres and count rows
    table_row_counts = {}
    for df, table_name in zip(dataframes, Table_Names):
        df.to_sql( name=table_name, con=postgres_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
       
    # Count rows in postgres table
        row_count = postgres_hook.get_first(f"SELECT COUNT(*) FROM {table_name}")[0]
        table_row_counts[table_name] = row_count

    # Using xcom to push row_count in postgres
    kwargs['ti'].xcom_push(key = 'postgres_row_count', value= table_row_counts)   
         
    # print(f"Row count in table {table_name}: {row_count}")
    
    # Count rows in DataFrame
    df_count_dict = {}
    for df, table_name in zip(dataframes, Table_Names):
        df_count = df.shape[0]
        df_count_dict[table_name] = df_count
    
    # Using xcom to push row_count 
    kwargs['ti'].xcom_push(key = 'df_row_count', value= df_count_dict)     
    
#Task compare table rows
def compare_table_rows(**kwargs):
    xcom_task_instance = kwargs['ti']
    postgres_row_count = xcom_task_instance.xcom_pull(task_ids='filter_and_load_data', key= 'postgres_row_count')
    df_row_count = xcom_task_instance.xcom_pull(task_ids='filter_and_load_data', key= 'df_row_count')

    comparison_results = {}
    
    for table_name, df_count in df_row_count.items():

        postgres_count = postgres_row_count.get(table_name, 0)
        
        comparison_results[table_name] = {
            'DataFrame Count': df_count,
            'Postgres Count': postgres_count,
            'Match': df_count == postgres_count
        }

    for key, value in comparison_results.items():
        print(f"Key: {key}, Value: {value}")
        print()
 
# Task to show DAG info
def dagrun_info(**kwargs):
    # Get the execution date and end date of the current dag run
    execution_date = kwargs['execution_date']
    end_date = datetime.now()
    # Get all task instances for the current dag run
    ti = kwargs['ti']
    durations = {}
    for task_instance in ti.get_dagrun().get_task_instances():
        if task_instance.duration is not None:  # Exclude tasks with None duration
            durations[task_instance.task_id] = task_instance.duration

    longest_task = max(durations, key=durations.get)
    longest_duration = durations[longest_task]
    shortest_task = min(durations, key=durations.get)
    shortest_duration = durations[shortest_task]

    print(f"DAG Start Time: {execution_date}")
    print(f"DAG End Time: {end_date}")
    for key,value in durations.items():
        print(f"Task {key} executed with duration: {value}")
    print(f"The task with the longest runtime is: {longest_task} with duration: {longest_duration}")
    print(f"The task with the shortest runtime is: {shortest_task} with duration: {shortest_duration}")


# Sensor to check for raw data file
check_raw_data_sensor = PythonSensor(
    task_id='check_raw_data_sensor',
    python_callable = check_for_raw_data,
    mode='poke',  # Continue to check until file is found or max retries is reached
    timeout=900,  # Max wait time: 15 minutes
    poke_interval=300,  # Check every 5 minutes
    dag=dag,
)

create_table = PostgresOperator(
    task_id = 'create_table_task',
    postgres_conn_id= 'postgres_conn',
    sql = sql_create_table,
    dag=dag,
)

filter_and_load_task = PythonOperator(
    task_id='filter_and_load_data',
    python_callable=filter_and_load_data,
    dag=dag,
)

check_table_rows_task = PythonOperator(
    task_id='check_table_rows_task',
    python_callable=compare_table_rows,
    provide_context=True,
    dag=dag,
)
dag_time_run = PythonOperator(
    task_id='dag_task_infomation',
    python_callable=dagrun_info,
    dag=dag,)

#Define dependencies

check_raw_data_sensor >> create_table >> filter_and_load_task >> check_table_rows_task >> dag_time_run


