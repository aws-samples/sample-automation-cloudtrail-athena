import os
import boto3
import re
import time

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

        # Finding if there are tables created already.
        athena_accounts = list_athena_tables_and_extract_accounts(data_catalog, database)
        # Finding accounts with logs in S3.
        accounts_ids = getting_accounts(bucket_name, prefix)

        # compare_account_lists(athena_accounts, accounts_ids)
        new_accounts = compare_account_lists(athena_accounts, accounts_ids)

        if new_accounts:
            run_athena(new_accounts, bucket_name, prefix, athena_results_bucket_name, database)
            print("New accounts found, running Athena queries")
        else:
            print("No new accounts found, skipping Athena queries")

        return print('Completed')
    
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise



def compare_account_lists(athena_accounts, accounts_ids):
    athena_set = set(athena_accounts)
    s3_set = set(accounts_ids)

    formatted_new_accounts = s3_set - athena_set
    # Format the new accounts as a string representation of a list

    if formatted_new_accounts:
        return formatted_new_accounts
    else:
        return False


def list_athena_tables_and_extract_accounts(data_catalog, database):
    # List tables in the specified database
    tables = []
    paginator = glue_client.get_paginator('get_tables')

    # Pattern for trail_ followed by exactly 12 digits
    pattern = 'trail_[0-9]{12}'

    try:
        page_iterator = paginator.paginate(
            DatabaseName=database,
            CatalogId=data_catalog,
            Expression=pattern
        )

        for page in page_iterator:
            tables.extend(page['TableList'])

    except Exception as e:
        print(f"Error getting tables: {str(e)}")
        raise

    # Extract account IDs from table names
    account_ids = []
    for table in tables:
        table_name = table['Name']
        if table_name.startswith('trail_'):
            account_id = table_name.split('_')[1]
            if account_id.isdigit() and len(account_id) == 12:  # Ensure it's a valid AWS account ID
                account_ids.append(account_id)

    # Remove duplicates and sort
    athena_account_ids = sorted(list(set(account_ids)))

    return athena_account_ids


def getting_accounts(bucket_name, prefix):

    # List objects in the bucket with the given prefix
    response_accounts = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    # Extract account IDs from the object keys
    account_ids = set()
    for obj in response_accounts.get('CommonPrefixes', []):
        match = re.match(r'^{}(\d+)/'.format(prefix), obj['Prefix'])
        if match:
            account_ids.add(match.group(1))

    return account_ids


def run_athena(accounts_ids, bucket_name, prefix, athena_results_bucket_name, database):
    for account_id in accounts_ids:
        # Specify the prefix (folder path) to list
        region = '''{region}'''
        date = '''{date}'''
        query_string = f"""
              CREATE EXTERNAL TABLE trail_{account_id} (
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
              COMMENT 'CloudTrail Organizational table for aws-controltower-logs-{account_id} bucket'
              PARTITIONED BY (region STRING, date STRING)
              ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
              STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
              LOCATION 's3://{bucket_name}/{prefix}{account_id}/CloudTrail/'
              TBLPROPERTIES (
              'projection.enabled'='true',
              'projection.region.type'='enum',
              'projection.region.values'='us-east-2,us-east-1,us-west-1,us-west-2,af-south-1,ap-east-1,ap-south-1,ap-northeast-3,ap-northeast-2,ap-southeast-1,ap-southeast-2,ap-northeast-1,ca-central-1,eu-central-1,eu-west-1,eu-west-2,eu-south-1,eu-west-3,eu-north-1,me-south-1,sa-east-1',
              'projection.date.format'='yyyy/MM/dd',
              'projection.date.interval'='1',
              'projection.date.interval.unit'='DAYS',
              'projection.date.range'='2015/01/01,NOW',
              'projection.date.type'='date',
              'storage.location.template'='s3://{bucket_name}/{prefix}{account_id}/CloudTrail/${region}/${date}');
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

        # Get the query execution ID
        print(f"Query {response} Executed to add table for {account_id}. Review your Athena Logs for status")
