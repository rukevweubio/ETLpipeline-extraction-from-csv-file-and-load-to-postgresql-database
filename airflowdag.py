from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import pandas as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator



# Conversion function
def convert_temperature(kelvin_temp):
    kelvin_temperature = (kelvin_temp - 273.15) * (9/5) + 32
    return kelvin_temperature


# Data transformation function
def transformation_data(task_instance):
    data = task_instance.xcom_pull(task_ids='get_op_response_filter')  # Corrected XCom pull

    temp_max = convert_temperature(data['main']['temp_max'])
    temp_min = convert_temperature(data['main']['temp_min'])
    temp = convert_temperature(data['main']['temp'])
    feels_like = convert_temperature(data['main']['feels_like'])
    
    # Extract other fields
    transformation_data = {
        'city': data['name'],
        'lon': data['coord']['lon'],
        'lat': data['coord']['lat'],
        'description': data['weather'][0]['description'],
        'temp': temp,
        'feels_like': feels_like,
        'temp_min': temp_min,
        'temp_max': temp_max,
        'pressure': data['main']['pressure'],
        'humidity': data['main']['humidity'],
        'sea_level': data['main'].get('sea_level', None),
        'grnd_level': data['main'].get('grnd_level', None),
        'visibility': data.get('visibility', None),
        'speed': data['wind']['speed'],
        'country': data['sys']['country'],
        'sunset': data['sys']['sunset'],
        'sunrise': data['sys']['sunrise'],
        'timezone': data.get('timezone', None),
    }
    
    df_data = pd.DataFrame([transformation_data])
    now = datetime.now()
    df_string = now.strftime("%Y%d%m_%H%M%S")
    file_name = f"current_weather_data_time_{df_string}.csv"
    df_data.to_csv(file_name, index=False)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2024, 9, 21),
    'email': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    'openweather_api_key',
    default_args=default_args,
    description='Data extraction from API cloud',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # HttpSensor to check if the API is ready
    weather_api_ready = HttpSensor(
        task_id='check_for_api_ready',
        http_conn_id='weather_api',
        endpoint='data/2.5/weather?q=Portland&appid=a2180e57be5343b5899f0275712555d4',
        timeout=20,
        retries=3,
        poke_interval=5
    )
    
    # SimpleHttpOperator to get the API response
    task_get_op_response_filter = SimpleHttpOperator(
        task_id="get_op_response_filter",
        http_conn_id='weather_api',
        method="GET",
        endpoint="data/2.5/weather?q=Portland&appid=a2180e57be5343b5899f0275712555d4",
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    # PythonOperator to transform the data
    extract_data_from_api_data = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=transformation_data
       # Ensure that context is passed to te function
    )

    # Create a GCS bucket
    create_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name='airflowb2',  # Correct spelling and ensure it's a string
        storage_class="REGIONAL",
        location="europe-west1",
        gcp_conn_id="google_cloud_default"
    )

    # Upload the file to Google Cloud Storage
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_file_google_bucket',
        src='/home/ubioworo/airflow/dummy_script/*.csv',
        dst='airflowb2/weather_api_data',
	bucket='airflowb2',  # Update with your bucket path
        gcp_conn_id='google_cloud_default'
    )

    # DummyOperator as a starting point
    start_task = DummyOperator(task_id='start_task')

    # Set task dependencies
    start_task >> weather_api_ready >> task_get_op_response_filter >> extract_data_from_api_data >> create_bucket >> upload_task
