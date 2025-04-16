# CloudTrail Logs Athena Table Management

## Overview

This CloudFormation template automates the creation and management of Athena tables for CloudTrail logs across multiple accounts in an AWS Organization. It deploys two Lambda functions, EventBridge rules, IAM roles, and necessary permissions to create and maintain Athena tables for efficient querying of CloudTrail logs.

## Supported Log Paths

This solution supports two different path structures for CloudTrail logs:

1. Control Tower central archiving account logs:
   `S3://<bucket_name>/org-id/AWSLogs/org-id/account_id/`

2. CloudTrail Trails organization logs:
   `S3://<bucket_name>/AWSLogs/account_id/`

The solution automatically detects and handles both path structures, ensuring compatibility with various AWS log configurations.


## Key Components

1. **Lambda Functions**:
   - `CloudTrailLogsPartitionedByAccount`: Creates Athena tables for individual account logs.
   - `CloudTrailLogsPartitionedAllAccounts`: Creates and maintains a combined Athena table for all accounts.

2. **EventBridge Rules**:
   - `LambdaTriggerRule`: Triggers the Lambda functions when they are first created.
   - `DailyLambdaTriggerRule`: Runs the Lambda functions daily at 1 AM EST.

3. **IAM Roles and Policies**: Provides necessary permissions for Lambda functions to access S3, Athena, Glue, and KMS.

4. **Glue Database**: Creates a Glue database to hold the Athena tables.

## Functionality

The solution performs the following tasks:

1. Creates Athena tables for CloudTrail logs from individual accounts.
2. Creates a combined Athena table for querying logs across all accounts.
3. Uses Athena partition projection for efficient querying and automated partition management.
4. Automatically updates tables when new accounts are added to the organization.
5. Runs daily to ensure all tables are up-to-date.

## Benefits

- **Centralized Log Analysis**: Query CloudTrail logs from multiple accounts in a single Athena table.
- **Efficient Querying**: Utilizes Athena partition projection for improved query performance.
- **Automated Management**: Automatically creates and updates tables as new accounts are added.
- **Cost-Effective**: Access and query your logs via Athena, paying only for the amount of data scanned and log storage. This      eliminates the need for costly third-party tools or additional licenses, significantly reducing overall expenses for log analysis.

## Deployment

To deploy this solution:

1. Review and customize the CloudFormation template parameters as needed.
2. Deploy the template in your AWS account.
3. Verify the creation of Lambda functions, EventBridge rules, and IAM roles.
4. Monitor the initial execution of the Lambda functions.

## Parameters

- `BucketName`: The name of the S3 bucket containing CloudTrail logs.(e.g., aws-controltower-logs-account-us-east-1.)
- `Prefix`: The path within the S3 bucket where CloudTrail logs are stored.(e.g., AWSLogs/ or ORG_ID/AWSLogs/ORG_ID/ ).
- `WorkgroupOutputLocationName`: The bucket name for Athena query results. (e.g., aws-athena-query-results-account-us-east-1.)
- `AthenaDatabase`: The name of the Athena database to hold the log tables.(e.g., ct-central.)

## Best Practices

- Test thoroughly in a non-production environment before deploying to production.
- Regularly review and audit the IAM permissions granted to the Lambda functions.
- Monitor Lambda function executions and Athena query performance.
- Implement appropriate data retention policies for your CloudTrail logs.

## Limitations

- The solution assumes a specific structure for CloudTrail logs in S3.
- It's designed for use with AWS Organizations and may require modifications for other setups.

## Security

This solution grants necessary permissions to Lambda functions to access and manage resources. Review and adjust the IAM policies as needed for your security requirements.

## License

This project is licensed under the MIT-0 License. See the LICENSE file for details.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute to this project.