# Amazon S3 bucket
import {
  to = aws_s3_bucket.hdmf
  id = "dey-hdmf"
}

# S3 notification configuration
import {
  to = aws_s3_bucket_notification.hdmf_lambda_trigger
  id = "dey-hdmf"
}

# AWS Lambda
import {
  to = aws_lambda_function.trl_processor
  id = "dey-trl"
}

# AWS Glue job
import {
  to = aws_glue_job.hdmf
  id = "dey-hdmf-glue"
}

# AWS Glue connection
import {
  to = aws_glue_connection.snowflake
  id = "956304645529:dey-hdmf-snowflake"
}

# Snowflake database
import {
  to = snowflake_database.hdmf
  id = "\"HDMF_MIGRATION_DB\""
}

# Snowflake warehouse
import {
  to = snowflake_warehouse.hdmf
  id = "\"HDMF_MIGRATION_WH\""
}

# Snowflake role
import {
  to = snowflake_account_role.hdmf
  id = "\"HDMF_MIGRATION_ROLE\""
}

# Snowflake schemas
import {
  to = snowflake_schema.staging
  id = "\"HDMF_MIGRATION_DB\".\"STAGING\""
}

import {
  to = snowflake_schema.curated
  id = "\"HDMF_MIGRATION_DB\".\"CURATED\""
}

import {
  to = snowflake_schema.audit
  id = "\"HDMF_MIGRATION_DB\".\"AUDIT\""
}

import {
  to = snowflake_schema.control
  id = "\"HDMF_MIGRATION_DB\".\"CONTROL\""
}
