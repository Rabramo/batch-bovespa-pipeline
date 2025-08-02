import boto3 # AWS SDK for Python
import os

def lambda_handler(event, context):
    """
    Lambda handler triggered by S3 PUT event.
    Starts the Glue Job defined in the environment variable GLUE_JOB_NAME.
    """
    glue = boto3.client('glue')
    
    job_name = os.environ.get('GLUE_JOB_NAME')
    if not job_name:
        raise Exception("Missing environment variable: GLUE_JOB_NAME")
    
    print("Event received:", event)                    

    response = glue.start_job_run(JobName=job_name)
    print(f"Started Glue Job: {job_name}, Run ID: {response['JobRunId']}")

    return {
        'statusCode': 200,
        'body': f"Glue Job: {job_name} started successfully."
    }
    
    