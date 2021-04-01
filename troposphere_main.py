import json

import stepfunctions as sf
import troposphere.iam as iam
from stepfunctions.steps import Fail, Catch, ChoiceRule
from troposphere import Join, serverless, s3, Sub, AWS_ACCOUNT_ID, Ref
from troposphere import Template
from troposphere.cloudformation import AWSCustomObject
from troposphere.glue import ExecutionProperty, JobCommand, Job, Database, \
    DatabaseInput, Crawler, SchemaChangePolicy, S3Target, Targets
from troposphere.stepfunctions import StateMachine

template = Template()
template.set_version("2010-09-09")
template.set_transform('AWS::Serverless-2016-10-31')
template.set_description("Example")

#### Internal S3 Bucket ####
internal_s3_bucket = template.add_resource(s3.Bucket(
    "DatalakeS3Bucket",
    BucketName=Sub(f'${{{AWS_ACCOUNT_ID}}}-test-stepfunctions-troposphere-glue'),
    PublicAccessBlockConfiguration=s3.PublicAccessBlockConfiguration(
        BlockPublicAcls=True,
        BlockPublicPolicy=True,
        IgnorePublicAcls=True,
        RestrictPublicBuckets=True
    )
))

custom_resource_empty_on_delete_execution_role = template.add_resource(
    iam.Role(
        "CustomResourceEmptyDatalakeBucketOnDeleteExecutionRole",
        RoleName='test-stepfunctions-troposphere-glue-empty-bucket-cr-role',
        AssumeRolePolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": ["lambda.amazonaws.com"]
                    },
                    "Action": ["sts:AssumeRole"]
                }
            ]
        },
        Path="/",
        ManagedPolicyArns=[
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        ]
    ))

custom_resource_empty_datalake_bucket_on_delete = template.add_resource(
    serverless.Function(
        "CustomResourceEmptyDatalakeBucketOnDelete",
        CodeUri="./clean_bucket",
        Description="Custom Resource Empty DatalakeBucket OnDelete",
        FunctionName="empty-example-s3-bucket-stepfunctions-glue-troposphere",
        Handler="clean_bucket.lambda_handler",
        Role=custom_resource_empty_on_delete_execution_role.get_att('Arn'),
        Runtime="python3.8",
        Timeout=900
    ))


class CleanupBucket(AWSCustomObject):
    resource_type = "Custom::cleanupbucket"
    props = {'ServiceToken': (str, True)}


cleanup_datalake_bucket_on_delete = template.add_resource(CleanupBucket(
    "CleanupDatalakeBucketOnDelete",
    ServiceToken=custom_resource_empty_datalake_bucket_on_delete.get_att('Arn'),
    BucketName=internal_s3_bucket.ref(),
    DependsOn=custom_resource_empty_datalake_bucket_on_delete,
))

####
#### Glue job troposphere definition #####

jobs_role = template.add_resource(iam.Role(
    'JobsRole',
    RoleName='stepfunctions-glue-troposphere-jobs-rl',
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    },
    ManagedPolicyArns=["arn:aws:iam::aws:policy/AdministratorAccess"]
))

job = template.add_resource(Job(
    'job',
    Command=JobCommand(
        Name="glueetl",
        ScriptLocation="./glue/import_from_s3.py",
        PythonVersion="3"
    ),
    DefaultArguments={
        "--TempDir": Sub("s3://${bucket}/temp", bucket=internal_s3_bucket.ref()),
        "--enable-metrics": "",
        "--job-language": "python"
    },
    ExecutionProperty=ExecutionProperty(
        MaxConcurrentRuns=1
    ),
    GlueVersion='2.0',
    MaxRetries=0,
    Name="s3-bucket-stepfunctions-glue-troposphere-job",
    NumberOfWorkers=2,
    Role=jobs_role.ref(),
    Timeout=300,
    WorkerType='G.1X',
))

#### Glue Database ####
glue_db = template.add_resource(Database(
    'Db',
    CatalogId=Ref(AWS_ACCOUNT_ID),
    DatabaseInput=DatabaseInput(
        Description="LAKE_ANALYTIC",
        Name='stepfunctions-glue-troposphere-db',
    )
))

#### Glue crawler ####
crawler_role = template.add_resource(iam.Role(
    'CrawlerRole',
    RoleName='stepfunctions-glue-troposphere-crawler-rl',
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    },
    ManagedPolicyArns=["arn:aws:iam::aws:policy/AdministratorAccess"]
))
crawler = template.add_resource(Crawler(
    'Crawler',
    DatabaseName=glue_db.ref(),
    Name='stepfunctions-glue-troposphere-crawler',
    Role=crawler_role.get_att('Arn'),
    Targets=Targets(
        S3Targets=[S3Target(
            Path=Sub(f's3://${{{internal_s3_bucket.title}}}/coviddatalake')
        )]
    ),
    SchemaChangePolicy=SchemaChangePolicy(
        DeleteBehavior='DEPRECATE_IN_DATABASE',
        UpdateBehavior='UPDATE_IN_DATABASE'
    )
))

### Start- Check crawler lambdas ######

lambda_crawler_execution_role = template.add_resource(
    iam.Role(
        "LambdaLGlueCrawlerExecutionRole",
        RoleName='stepfunctions-glue-troposphere-lambda-role',
        AssumeRolePolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": ["lambda.amazonaws.com"]
                    },
                    "Action": ["sts:AssumeRole"]
                }
            ]
        },
        Path="/",
        ManagedPolicyArns=[
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        ]
    ))

start_crawler_lambda = template.add_resource(
    serverless.Function(
        "startCrawlerLambda",
        CodeUri="./start_crawler",
        Description="start-crawler",
        FunctionName='stepfunctions-glue-troposphere-start-lambda',
        Handler="handler.lambda_handler",
        Role=lambda_crawler_execution_role.get_att('Arn'),
        Runtime="python3.8",
        Timeout=30
    ))

check_crawler_lambda = template.add_resource(
    serverless.Function(
        "checkCrawlerLambda",
        CodeUri="./check_crawler",
        Description="check-crawler-status",
        FunctionName='stepfunctions-glue-troposphere-check-lambda',
        Handler="handler.lambda_handler",
        Role=lambda_crawler_execution_role.get_att('Arn'),
        Runtime="python3.8",
        Timeout=30
    ))

### Step Function ####

fail_state = Fail("DataImportFailed")
finish = sf.steps.Succeed(state_id='Finish')

glue_import_to_s3_job = sf.steps.GlueStartJobRunStep(
    state_id='Import to s3 raw',
    wait_for_completion=True,
    timeout_seconds=7200,
    comment="Import database to s3",
    parameters={
        "JobName": "${jobName}",
        "Arguments": {
            "--s3_external_bucket": "${s3ExternalBucket}",
            "--s3_internal_bucket": "${s3InternalBucket}",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-s3-parquet-optimized-committer": "true",
            "--job-bookmark-option": "job-bookmark-disable"
        }
    }
)
glue_import_to_s3_job.add_retry(sf.steps.Retry(error_equals=["Glue.AWSGlueException"]))
glue_import_to_s3_job.add_catch(Catch(
    error_equals=["States.TaskFailed"],
    next_step=fail_state
))

crawl_params = sf.steps.Pass(
    state_id='Crawl s3 Params',
    result={
        'crawlerName': "${crawlerName}"
    }
)

crawl_database_start = sf.steps.Task(
    state_id='start crawler',
    resource='${startCrawlerLambda}'
)
crawl_database_start.add_retry(
    sf.steps.Retry(error_equals=[
        "Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"
    ])
)
crawl_database_start.add_catch(Catch(
    error_equals=["States.TaskFailed"],
    next_step=fail_state
))

crawl_database_wait = sf.steps.Wait(
    state_id='wait for crawler',
    seconds=20
)

crawl_database_check_status = sf.steps.Task(
    state_id='check crawler status',
    resource='${checkCrawlerLambda}'
)
crawl_database_check_status.add_retry(
    sf.steps.Retry(error_equals=[
        "Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"
    ])
)
crawl_database_check_status.add_catch(Catch(
    error_equals=["States.TaskFailed"],
    next_step=fail_state
))

crawl_database_check_choice = sf.steps.Choice(
    state_id='Check finished?',
)

crawl_database_check_choice.add_choice(
    ChoiceRule.BooleanEquals('$.crawlerStatus', False),
    crawl_database_wait
)
crawl_database_check_choice.add_choice(
    ChoiceRule.BooleanEquals('$.crawlerStatus', True),
    finish
)

workflow_definition = sf.steps.Chain([
    glue_import_to_s3_job,
    crawl_params,
    crawl_database_start,
    crawl_database_wait,
    crawl_database_check_status,
    crawl_database_check_choice
])

workflow = sf.workflow.Workflow(
    name="workflow",
    definition=workflow_definition,
    role='placeholder'
)

stepfunction_role = template.add_resource(iam.Role(
    'StepfunctionRole',
    RoleName='test-step-function-rl',
    ManagedPolicyArns=['arn:aws:iam::aws:policy/AdministratorAccess'],
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
))

ingestion_step_function = template.add_resource(StateMachine(
    'ingestionStatMachine',
    StateMachineName='test-stepfunctions-glue',
    StateMachineType='STANDARD',
    DefinitionString=workflow.definition.to_json(),
    RoleArn=stepfunction_role.get_att('Arn'),
    DefinitionSubstitutions={
        "s3ExternalBucket": "s3://covid19-lake/uk_covid/json",
        "s3InternalBucket": Sub(f's3://${{{internal_s3_bucket.title}}}/coviddatalake'),
        "crawlerName": crawler.ref(),
        "jobName": job.ref(),
        "startCrawlerLambda": start_crawler_lambda.get_att('Arn'),
        "checkCrawlerLambda": check_crawler_lambda.get_att('Arn')
    }
))

with open('troposphere_main.json', 'w') as f:
    f.write(template.to_json())
