HDMF Data Migration – Step-by-Step Process

Step 1: Review the Architecture

The migration follows this flow:

Amazon S3
    ↓
AWS Lambda
    ↓
AWS Glue
    ↓
Snowflake

Amazon S3 stores the raw and processed files.

AWS Lambda checks and processes the uploaded .trl files.

AWS Glue transforms the validated data.

Snowflake stores the final staging, curated, audit, migration log, and validation tables.

Step 2: Prepare the Amazon S3 Bucket

Open the Amazon S3 console and use the bucket:

dey-hdmf

Create or verify the following folders:

raw/
dq-passed/
dq-rejected/
dq-skipped/
logs/

Step 3: Upload the .trl Files

Upload all HDMF .trl files to:

s3://dey-hdmf/raw/

A total of 13 source files should be uploaded.

Step 4: Run the AWS Lambda Processing

Open the AWS Lambda function:

dey-trl

The Lambda function should automatically run through the Amazon S3 trigger.

It performs the following tasks:

Reads the uploaded .trl files.

Checks whether each file contains data.

Parses the binary records.

Separates passed, rejected, and skipped files.

Writes processing logs to Amazon S3.

Valid files are written to:

s3://dey-hdmf/dq-passed/

Zero-byte files are written to:

s3://dey-hdmf/dq-skipped/

The seven zero-byte files should be recorded as:

Status: SKIPPED
Risk Flag: RISK-01
Records Parsed: 0

Step 5: Verify the Amazon S3 Outputs

Check that the processed files are available under:

dq-passed/
dq-rejected/
dq-skipped/
logs/

Verify that:

Six files with data were passed.

Seven zero-byte files were skipped.

No files failed because of a processing error.

Step 6: Prepare the Snowflake Environment

Log in to Snowflake and open:

Projects → Worksheets

Create a new SQL worksheet and run:

USE ROLE HDMF_MIGRATION_ROLE;
USE WAREHOUSE HDMF_MIGRATION_WH;
USE DATABASE HDMF_MIGRATION_DB;

Check the available schemas:

SHOW SCHEMAS IN DATABASE HDMF_MIGRATION_DB;

Expected schemas:

STAGING
CURATED
AUDIT
CONTROL

Step 7: Test the AWS Glue Connection to Snowflake

Open AWS Glue and go to:

Data connections

Select the connection:

dey-hdmf-snowflake

Run the connection test.

The test must be successful before running the Glue job.

Step 8: Run the AWS Glue Job

Open the AWS Glue job:

dey-hdmf-glue

Run the job.

The Glue job performs the following tasks:

Reads the validated data from the dq-passed folder in Amazon S3.

Transforms and organizes the records.

Loads the raw events into Snowflake staging tables.

Loads the reference data into the audit table.

Applies SCD Type 2 to the transaction tables.

Creates the migration log and validation results.

Loads all results into Snowflake.

Wait until the job status becomes:

Succeeded

Step 9: Open Snowflake

Log in to Snowflake and open a SQL worksheet.

Set the role, warehouse, and database:

USE ROLE HDMF_MIGRATION_ROLE;
USE WAREHOUSE HDMF_MIGRATION_WH;
USE DATABASE HDMF_MIGRATION_DB;

Step 10: Check the Snowflake Schemas

SHOW SCHEMAS IN DATABASE HDMF_MIGRATION_DB;

Expected schemas:

STAGING
CURATED
AUDIT
CONTROL

Step 11: Check the Snowflake Tables

SHOW TABLES IN SCHEMA HDMF_MIGRATION_DB.STAGING;
SHOW TABLES IN SCHEMA HDMF_MIGRATION_DB.CURATED;
SHOW TABLES IN SCHEMA HDMF_MIGRATION_DB.AUDIT;
SHOW TABLES IN SCHEMA HDMF_MIGRATION_DB.CONTROL;

Step 12: Check the Migration Log

SELECT *
FROM HDMF_MIGRATION_DB.CONTROL.MIGRATION_LOG;

Step 13: Check the File Status Summary

SELECT STATUS, COUNT(*) AS FILE_COUNT
FROM HDMF_MIGRATION_DB.CONTROL.MIGRATION_LOG
GROUP BY STATUS;

Expected result:

COMPLETE: 6
SKIPPED: 7
FAILED: 0

Step 14: Check the RISK-01 Files

SELECT SOURCE_FILE, STATUS, RISK_FLAG
FROM HDMF_MIGRATION_DB.CONTROL.MIGRATION_LOG
WHERE RISK_FLAG = 'RISK-01';

The query should return all seven zero-byte source files.

Step 15: Check the Staging Tables

SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_LO_STL_PURPOSE;
SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_LO_STL_FRONTEND;
SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_LO_STL_ONLINE_APPLICATION;
SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_LMS_NONCASH_COLLECTION;
SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_LMS_STL_DISBURSEMENT_MASTER;
SELECT COUNT(*) FROM HDMF_MIGRATION_DB.STAGING.STG_PF_EMPLOYER_MASTER;

Expected total staging records:

215

Step 16: Check the Audit Table

SELECT COUNT(*)
FROM HDMF_MIGRATION_DB.AUDIT.AUDIT_LO_STL_PURPOSE;

Expected audit records:

30

Step 17: Check the Curated Tables

SHOW TABLES IN SCHEMA HDMF_MIGRATION_DB.CURATED;

The curated tables should contain the historical transaction records processed using SCD Type 2.

Step 18: Check the Validation Results

SELECT *
FROM HDMF_MIGRATION_DB.CONTROL.VALIDATION_RESULTS;

Expected result:

Total checks: 42
Passed: 42
Failed: 0
Overall: PASS

Step 19: Check for Sensitive Files Before GitHub Upload

Open the local project folder:

cd ~/hdmf/HDMF_Final_Submission

Check for private keys, credentials, and secret files:

find . -type f \( \
  -name "*.p8" -o \
  -name "*.pem" -o \
  -name "*.pub" -o \
  -name ".env" -o \
  -name "*private_key*" -o \
  -name "*public_key*" -o \
  -name "*secret*.json" \
\)

Do not upload any AWS credentials, Snowflake private keys, or secret files.

Step 20: Check Git Status

git status

Step 21: Add the Updated README

Make sure this file is named:

README.md

Then run:

git add README.md

Step 22: Commit the README Update

git commit -m "docs: update Snowflake migration steps"

Step 23: Push to GitHub

git push

Step 24: Final Verification

Verify that:

The AWS Lambda processing completed successfully.

The AWS Glue job status is Succeeded.

Snowflake contains the STAGING, CURATED, AUDIT, and CONTROL schemas.

The migration log contains all 13 source files.

Six files are marked COMPLETE.

Seven files are marked SKIPPED with RISK-01.

No files are marked FAILED.

All 42 validation checks passed.

The GitHub repository is set to Private.

No credentials or private keys were uploaded.

Final Result

13 source files processed
6 completed
7 skipped with RISK-01
0 failed
42 validation checks passed
Overall result: PASS