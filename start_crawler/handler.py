from copy import deepcopy
import boto3

glue = boto3.client(service_name='glue')


def lambda_handler(event, context):
    print(event)
    result = deepcopy(event)
    print("Starting Glue Crawler")
    response = glue.start_crawler(Name=event['crawlerName'])
    return result
