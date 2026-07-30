
resource "aws_s3_bucket" "hdmf" {
  bucket        = "dey-hdmf"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}



# AWS LAMBDA FUNCTION

resource "aws_lambda_function" "trl_processor" {
  function_name = "dey-trl"
  role          = "arn:aws:iam::956304645529:role/dey-ta-hdmf"

  package_type = "Zip"
  runtime      = "python3.14"
  handler      = "lambda_function.lambda_handler"

  architectures = ["x86_64"]
  memory_size   = 512
  timeout       = 300

  reserved_concurrent_executions = -1

  filename         = "${path.module}/lambda_existing.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda_existing.zip")

  lifecycle {
    prevent_destroy = true

    # Keep the existing Lambda environment variables and
    # deployed configuration unchanged during import.
    ignore_changes = [
      environment,
      filename,
      source_code_hash
    ]
  }
}



# AMAZON S3 TRIGGER FOR LAMBDA

resource "aws_s3_bucket_notification" "hdmf_lambda_trigger" {
  bucket      = aws_s3_bucket.hdmf.id
  eventbridge = false

  lambda_function {
    id                  = "15f01f2a-419d-4845-a007-8eaeac0b6b8b"
    lambda_function_arn = aws_lambda_function.trl_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".trl"
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}



# AWS GLUE CONNECTION TO SNOWFLAKE

resource "aws_glue_connection" "snowflake" {
  catalog_id      = "956304645529"
  name            = "dey-hdmf-snowflake"
  connection_type = "SNOWFLAKE"

  lifecycle {
    prevent_destroy = true

    # Preserve the existing Snowflake connection properties,
    # authentication configuration, secret references, and
    # network settings during import.
    ignore_changes = all
  }
}


# AWS GLUE JOB

resource "aws_glue_job" "hdmf" {
  name     = "dey-hdmf-glue"
  role_arn = "arn:aws:iam::956304645529:role/dey-ta-hdmf"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://dey-hdmf/glue-scripts/dey-hdmf-glue.py"
  }

  lifecycle {
    prevent_destroy = true

    # Preserve the existing Glue version, workers, arguments,
    # connections, timeout, and execution settings during import.
    ignore_changes = all
  }
}
