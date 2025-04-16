# CloudTrail Logs Athena Table Management Solution

## Overview

This solution automates the creation and management of Athena tables for CloudTrail logs across multiple accounts in an AWS Organization. It uses CloudFormation to deploy Lambda functions, EventBridge rules, and necessary IAM roles to create and maintain Athena tables for efficient querying of CloudTrail logs.

## Architecture

The solution consists of the following key components:

1. Two Lambda functions
2. Two EventBridge rules
3. IAM roles and permissions
4. A Glue Database

### Lambda Functions

1. **CloudTrailLogsPartitionedByAccount**
   - Creates Athena tables for individual account logs
   - Checks for new accounts and creates tables as needed

2. **CloudTrailLogsPartitionedAllAccounts**
   - Creates and maintains a combined Athena table for all accounts
   - Updates the combined table with new account data

### EventBridge Rules

1. **LambdaTriggerRule**
   - Triggers when the Lambda functions are created
   - Ensures initial setup of Athena tables upon deployment

2. **DailyLambdaTriggerRule**
   - Runs daily at 1 AM EST (6 AM UTC)
   - Invokes both Lambda functions for daily updates

## Execution Flow

### Initial Deployment

1. CloudFormation stack is created
2. LambdaTriggerRule is activated
3. Both Lambda functions are invoked to set up initial Athena tables

### Daily Execution

1. DailyLambdaTriggerRule runs at 1 AM EST
2. Both Lambda functions are invoked to update and maintain Athena tables

### Ongoing Maintenance

- CloudTrailLogsPartitionedByAccount:
  1. Determines the S3 prefix for CloudTrail logs
  2. Lists existing Athena tables and extracts account IDs
  3. Gets account IDs with logs in S3
  4. Compares S3 accounts with existing Athena tables
  5. Creates Athena tables for new accounts if found

- CloudTrailLogsPartitionedAllAccounts:
  1. Determines the S3 prefix for CloudTrail logs
  2. Gets all account IDs with logs in S3
  3. Checks if the combined table exists
  4. Updates existing table or creates a new one with all account IDs

## Key Features

- Automatic detection of new AWS accounts in the organization
- Creation and maintenance of individual account tables and a combined table
- Use of Athena partition projection for efficient querying
- Daily updates to ensure all tables are current
- Supports both Control Tower central archiving and CloudTrail Trails organization log paths

## Benefits

- Centralized log analysis across multiple AWS accounts
- Improved query performance through Athena partition projection
- Automated table management, reducing manual overhead
- Cost-effective solution using serverless components

## Deployment

Deploy this solution using the provided CloudFormation template. The template will create all necessary resources, including Lambda functions, IAM roles, and EventBridge rules.

## Customization

The solution can be customized by modifying the CloudFormation template or Lambda function code. Key parameters such as S3 bucket names, database names, and schedule can be adjusted to fit specific requirements.

## Monitoring and Maintenance

- Monitor Lambda function executions through CloudWatch Logs
- Review Athena query performance periodically
- Ensure S3 bucket policies and IAM roles remain up-to-date with organizational changes

## Security Considerations

- The solution uses IAM roles with least privilege principles
- Ensure proper S3 bucket policies are in place for log access
- Regularly review and audit the permissions granted to Lambda functions

## Conclusion

This solution provides an efficient, automated way to manage Athena tables for CloudTrail logs across multiple accounts in an AWS Organization. By automating table creation and updates, it enables easier and more efficient analysis of CloudTrail logs at scale.
