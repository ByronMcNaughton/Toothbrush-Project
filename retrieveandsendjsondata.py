import json
import boto3

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    bucket =  'xander-toothbrush-insight-project'
    key = 'apidata.txt'

    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    
    return json_data