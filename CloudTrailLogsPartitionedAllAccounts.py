import os
import boto3
import re
from botocore.exceptions import ClientError


# Create a boto3 session
session = boto3.Session()

# Get the current region from the session
current_region = session.region_name

# Create an S3 client
s3 = boto3.client('s3')
athena_client = boto3.client('athena', region_name=current_region)
glue_client = boto3.client('glue')

# Validation patterns
BUCKET_NAME_PATTERN = re.compile(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')
PREFIX_PATTERN = re.compile(r'^[\w\-\./]+$')
DATABASE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,64}$')

def validate_input(input_str: str, pattern: re.Pattern, max_length: int = 255) -> bool:
    """Validate input string against pattern and length."""
    if not isinstance(input_str, str):
        return False
    if len(input_str) > max_length:
        return False
    return bool(pattern.match(input_str))


def lambda_handler(event, context):
    try:
        bucket_name = os.environ['BucketName']
        athena_results_bucket_name = os.environ['AthenaResultsBucketName']
        database = os.environ['AthenaDatabase']
        prefix = os.environ['Prefix'].lstrip('/')
        data_catalog = os.environ['DataCatalog']

        # Validate inputs
        if not all([
            validate_input(bucket_name, BUCKET_NAME_PATTERN),
            validate_input(athena_results_bucket_name, BUCKET_NAME_PATTERN),
            validate_input(database, DATABASE_NAME_PATTERN),
            validate_input(prefix, PREFIX_PATTERN)
        ]):
            raise ValueError("Invalid input parameters")

        accounts_id = getting_accounts(bucket_name, prefix)

        # Check if all accounts table exists. If it does update the projection. If it doesn't, create it.
        try:
            # Check if table exists by attempting to get its metadata
            response = glue_client.get_table(
                DatabaseName=database,
                CatalogId=data_catalog,
                Name='all_accounts_trail'
            )

            if response:
                update_table(database, athena_results_bucket_name, accounts_id)

        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"Table all_accounts_trail does not exist in database {database}")
                run_athena(accounts_id, bucket_name, prefix, athena_results_bucket_name, database)
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise


def update_table(database, athena_results_bucket_name, accounts_id):
    query = f"""
              ALTER TABLE all_accounts_trail SET TBLPROPERTIES ('projection.account.values'={accounts_id})"""
    response = athena_client.start_query_execution(
        QueryString=str(query),
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{athena_results_bucket_name}/"
        }
    )

    print(f"Query {response} was executed. Table all_accounts_trail was updated. Review your Athena Tables for status")


def run_athena(accounts_id, bucket_name, prefix, athena_results_bucket_name, database):
    region = '''{region}'''
    date = '''{date}'''
    account = '''{account}'''
    query_string = f"""
              CREATE EXTERNAL TABLE all_accounts_trail (
                  eventVersion STRING,
                  userIdentity STRUCT<
                      type: STRING,
                      principalId: STRING,
                      arn: STRING,
                      accountId: STRING,
                      invokedBy: STRING,
                      accessKeyId: STRING,
                      userName: STRING,
                      sessionContext: STRUCT<
                          attributes: STRUCT<
                              mfaAuthenticated: STRING,
                              creationDate: STRING>,
                          sessionIssuer: STRUCT<
                              type: STRING,
                              principalId: STRING,
                              arn: STRING,
                              accountId: STRING,
                              username: STRING>,
                          ec2RoleDelivery: STRING,
                          webIdFederationData: MAP<STRING,STRING>>>,
                  eventTime STRING,
                  eventSource STRING,
                  eventName STRING,
                  awsRegion STRING,
                  sourceIpAddress STRING,
                  userAgent STRING,
                  errorCode STRING,
                  errorMessage STRING,
                  requestParameters STRING,
                  responseElements STRING,
                  additionalEventData STRING,
                  requestId STRING,
                  eventId STRING,
                  resources ARRAY<STRUCT<
                      arn: STRING,
                      accountId: STRING,
                      type: STRING>>,
                  eventType STRING,
                  apiVersion STRING,
                  readOnly STRING,
                  recipientAccountId STRING,
                  serviceEventDetails STRING,
                  sharedEventID STRING,
                  vpcEndpointId STRING,
                  tlsDetails STRUCT<
                      tlsVersion: STRING,
                      cipherSuite: STRING,
                      clientProvidedHostHeader: STRING>
              )
              COMMENT 'CloudTrail Organizational All Accounts'
                  PARTITIONED BY (`account` string, region STRING, date STRING)
              ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
              STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
              LOCATION 's3://{bucket_name}/{prefix}'
              TBLPROPERTIES (
              'projection.enabled'='true',
              'projection.account.type'='enum',
              'projection.account.values'={accounts_id},
              'projection.region.type'='enum',
              'projection.region.values'='us-east-2,us-east-1,us-west-1,us-west-2,af-south-1,ap-east-1,ap-south-1,ap-northeast-3,ap-northeast-2,ap-southeast-1,ap-southeast-2,ap-northeast-1,ca-central-1,eu-central-1,eu-west-1,eu-west-2,eu-south-1,eu-west-3,eu-north-1,me-south-1,sa-east-1',
              'projection.date.format'='yyyy/MM/dd', 
              'projection.date.interval'='1', 
              'projection.date.interval.unit'='DAYS', 
              'projection.date.range'='2015/01/01,NOW', 
              'projection.date.type'='date', 
              'storage.location.template'='s3://{bucket_name}/{prefix}${account}/CloudTrail/${region}/${date}')
              """

    # Start the Athena query
    response = athena_client.start_query_execution(
        QueryString=str(query_string),
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{athena_results_bucket_name}/"
        }
    )

    print(f"Query {response} was executed. Table all_accounts_trail was created. Review your Athena Tables for status")


def getting_accounts(bucket_name, prefix):
    # List objects in the bucket with the given prefix
    response_accounts = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    # Extract account IDs from the object keys
    account_ids = set()
    for obj in response_accounts.get('CommonPrefixes', []):
        match = re.match(r'^{}(\d+)/'.format(prefix), obj['Prefix'])
        if match:
            account_ids.add(match.group(1))

    # Join the account IDs into a comma-separated string
    account_ids_str = ','.join(account_ids)

    # Save the account IDs string in a variable
    account_values = str("'" + account_ids_str + "'")

    return account_values
