# Import necessary modules
import os
import io
import boto3
import openai
from airflow import DAG
from pathlib import Path
from dotenv import load_dotenv
from airflow.models.param import Param
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Set parameters for user input
user_input = {
        "user_sleep_timer": Param(30, type='integer', minimum=10, maximum=120),
        }

# Create DAG with the given parameters
dag = DAG(
    dag_id="Batch-DAG",
    schedule="0 5 * * *",   # https://crontab.guru/ - Runs every day at 5:00 AM
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params=user_input,
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

def read_audio_file_from_s3():
    """
    Read the audio files from AWS S3 bucket and return the list of file names
    """
    source_bucket = s3Res.Bucket(user_s3_bucket)
    destination_bucket = s3Res.Bucket(user_s3_bucket)

    for obj in source_bucket.objects.filter(Prefix='Adhoc-Folder/'):
        source_key = obj.key
        destination_key = 'Batch-Folder/' + source_key.split('/')[-1]
        destination_bucket.copy({'Bucket': obj.bucket_name, 'Key': source_key},
                                destination_key)
    
    s3_folder_name = "Batch-Folder/"
    s3_files = s3Client.list_objects(Bucket = user_s3_bucket, Prefix = s3_folder_name).get('Contents')
    file_list = []
    for s3_file in s3_files:
        file_path = s3_file['Key']
        file_path = file_path.split('/')
        if file_path[-1] != '':
            file_list.append(file_path[-1])
    
    if (len(file_list)!=0):
        return file_list

# Define a function to transcribe a media file using the Whispr API.
def transcribe_media_file():
    """
    Transcribe a media file using the Whispr API
    """
    file_list = read_audio_file_from_s3()
    text_list = []
    for file in file_list:
        s3_object_key = f'Batch-Folder/{file}'
        response = s3Client.get_object(Bucket=user_s3_bucket, Key=s3_object_key)
        audio_file = io.BytesIO(response['Body'].read())
        audio_file.name = s3_object_key
        # Transcribe the audio file using the OpenAI API
        transcript = openai.Audio.transcribe("whisper-1", audio_file)
        text = transcript["text"]
        text_list.append(text)
    return file_list, text_list

def transcript_file_s3():
    file_list, text_list = transcribe_media_file()
    for i in range(len(file_list)):
        s3_object_key = f'Batch-Folder/{file_list[i]}'
        filename = s3_object_key.split('/')[1].replace('.mp3','.txt')
        s3Client.put_object(Bucket = user_s3_bucket, Key = 'Processed-Text-Folder/'+filename, Body = text_list[i])

def gpt_default_questions():
    """
    GPT default questions
    """
    s3_folder_name = "Processed-Text-Folder/"
    s3_files = s3Client.list_objects(Bucket = user_s3_bucket, Prefix = s3_folder_name).get('Contents')
    file_list = []
    for s3_file in s3_files:
        file_path = s3_file['Key']
        file_path = file_path.split('/')
        if file_path[-1] != '':
            file_list.append(file_path[-1])
    
    answer_list = []
    for i in range(len(file_list)):
        prompt = f'Context: {file_list[i]}'+ '\n' + 'Given the transcript, Generate 3-4 default questionnaire on the selected transcript and generate answers for the same:'
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
        answer_list.append(answer)
    return answer_list

def push_answers_s3():
    answer_list = gpt_default_questions()
    s3_folder_name = "Batch-Folder/"
    s3_files = s3Client.list_objects(Bucket = user_s3_bucket, Prefix = s3_folder_name).get('Contents')
    file_list = []
    for s3_file in s3_files:
        file_path = s3_file['Key']
        file_path = file_path.split('/')
        if file_path[-1] != '':
            file_list.append(file_path[-1])
    
    for i in range(len(file_list)):
        s3_object_key = f'Batch-Folder/{file_list[i]}'
        filename = s3_object_key.split('/')[1].replace('.mp3','_answers.txt')
        s3Client.put_object(Bucket = user_s3_bucket, Key = 'GPT-Answers-Folder/'+filename, Body = answer_list[i])

with dag:
    read_audio_files = PythonOperator(   
    task_id='read_audio_files_s3',
    python_callable = read_audio_file_from_s3,
    provide_context=True,
    dag=dag,
    )

    transcript_file = PythonOperator(   
    task_id='transcript_file_s3',
    python_callable = transcript_file_s3,
    provide_context=True,
    dag=dag,
    )

    gpt_questions = PythonOperator(   
    task_id='gpt_question',
    python_callable = gpt_default_questions,
    provide_context=True,
    dag=dag,
    )

    push_answers = PythonOperator(   
    task_id='push_gpt_answers',
    python_callable = push_answers_s3,
    provide_context=True,
    dag=dag,
    )

    read_audio_files >> transcript_file >> gpt_questions >> push_answers