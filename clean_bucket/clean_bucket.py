import boto3
import json
import sys
import traceback
import urllib.request


def lambda_handler(event, context):
    print(json.dumps(event))
    try:
        bucket = event['ResourceProperties']['BucketName']
        if event['RequestType'] == 'Delete':
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket)
            bucket.objects.all().delete()
            send(event, context, "SUCCESS", {})
        else:
            send(event, context, "SUCCESS", {})
    except Exception as e:
        print(e)
        send(event, context, "FAILED", {})


def log_exception():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))


def send(event, context, responseStatus, resp_data):
    responseBody = {'Status': responseStatus,
                    'Reason': 'Log stream name: ' + context.log_stream_name,
                    'PhysicalResourceId': context.log_stream_name,
                    'StackId': event['StackId'],
                    'RequestId': event['RequestId'],
                    'LogicalResourceId': event['LogicalResourceId'],
                    'Data': resp_data}
    data = json.dumps(responseBody).encode('utf-8')
    req = urllib.request.Request(event['ResponseURL'], data, headers={'Content-Length': len(data), 'Content-Type': ''})
    req.get_method = lambda: 'PUT'
    try:
        with urllib.request.urlopen(req) as response:
            print(f'response.status: {response.status}, ' +
                  f'response.reason: {response.reason}')
            print('response from cfn: ' + response.read().decode('utf-8'))
    except urllib.error.URLError:
        log_exception()
        raise Exception('non-200')
    return True
