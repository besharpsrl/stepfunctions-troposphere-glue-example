import boto3
from copy import deepcopy

glue = boto3.client(service_name='glue')


def lambda_handler(event, context):
    print(event)
    response = glue.get_crawler_metrics(CrawlerNameList=[event['crawlerName']])
    result = deepcopy(event)
    if response['CrawlerMetricsList'][0]['StillEstimating']:
        result['crawlerStatus'] = False
    elif response['CrawlerMetricsList'][0]['TimeLeftSeconds'] > 0:
        result['crawlerStatus'] = False
    else:
        result['crawlerStatus'] = True
    return result
