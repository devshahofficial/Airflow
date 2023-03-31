import os
import io
import boto3
import openai
from airflow import DAG
from pathlib import Path
from dotenv import load_dotenv
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Create DAG with the given parameters
dag = DAG(
    dag_id="Adhoc-DAG",
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

# Create an AWS S3 client to store in user bucket
s3Client = boto3.client('s3',
                    region_name='us-east-1',
                    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                    aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                    )

# Create an AWS S3 Resource to access resources available in user bucket
s3Res = boto3.resource('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

# Create an AWS S3 log client to store all the logs in the log folder
s3ClientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOGS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOGS_SECRET_KEY')
                        )

#load env variables
dotenv_path = Path('./dags/.env')
load_dotenv(dotenv_path)
openai.api_key = os.environ.get('OPENAI_API_KEY')

# Defining User Bucket to store file
user_s3_bucket = "damg-7245-audio-transcripts"

def transcribe_media_file(s3_object_key, ti, **kwargs):
    response = s3Client.get_object(Bucket=user_s3_bucket, Key=s3_object_key)
    audio_file = io.BytesIO(response['Body'].read())
    audio_file.name = s3_object_key

    transcript = openai.Audio.transcribe("whisper-1", audio_file)
    text = transcript["text"]
    filename = s3_object_key.split('/')[-1].replace('.mp3','.txt')
    # Upload the transcript as a text file to the S3 bucket
    s3Client.put_object(Bucket=user_s3_bucket, Key='Processed-Text-Folder/' + filename, Body=text)
    ti.xcom_push(key="answers", value=text)

def gpt_default_questions(transcript, s3_object_key):
    prompt = f'Context: {transcript}\nGenerate 3-4 default questions on the selected transcript and generate answers for the same:'
    response = openai.Completion.create(
        engine='text-davinci-002',
        prompt=prompt,
        temperature=0.5,
        max_tokens=1024,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    answer = response.choices[0].text.strip()
    filename = s3_object_key.split('/')[1].replace('.mp3','_answers.txt')
    s3Client.put_object(Bucket = user_s3_bucket, Key = 'GPT-Answers-Folder/'+filename, Body = answer)

with dag:
    transcribe_audio_file = PythonOperator(
        task_id='transcribe_audio_file_s3',
        python_callable=transcribe_media_file,
        provide_context=True,
        op_kwargs = {'s3_object_key': '{{ dag_run.conf["s3_object_key"] }}',
                    'transcript': "{{task_instance.xcom_pull(task_ids='transcribe_media_file', key='text')}}"}
    )

    gpt_default_questions_task = PythonOperator(
        task_id='gpt_default_questions_task',
        python_callable=gpt_default_questions,
        op_kwargs = {'s3_object_key': '{{ dag_run.conf["s3_object_key"] }}',
                    'transcript': '{{ task_instance.xcom_pull(task_ids="transcribe_audio_file_task", key="text") }}'
                    }
    )
    
    transcribe_audio_file >> gpt_default_questions_task