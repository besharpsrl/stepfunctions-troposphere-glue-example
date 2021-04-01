import sys
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_external_bucket_path',
    's3_internal_bucket_path'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_external_bucket = args['s3_external_bucket_path']
s3_internal_bucket = args['s3_internal_bucket_path']

print(args)
source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [s3_external_bucket]
    },
    format="json"
).toDF()

print('converted to DF')

result = DynamicFrame.fromDF(source, glueContext, 'testres')

sink = glueContext.write_dynamic_frame.from_options(
    frame=result,
    connection_type="s3",
    connection_options={
        "path": s3_internal_bucket
    },
    format="glueparquet"
)
