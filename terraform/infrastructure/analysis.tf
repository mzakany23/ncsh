locals {
  analysis_function_name = "ncsoccer-analysis"
  analysis_image_tag    = "latest"
}

# ECR Repository for Analysis Lambda
resource "aws_ecr_repository" "analysis" {
  name = var.analysis_repository
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# IAM Role for Analysis Lambda
resource "aws_iam_role" "analysis_lambda" {
  name = "ncsoccer-analysis-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Create Secrets Manager secret
resource "aws_secretsmanager_secret" "ncsoccer" {
  name        = "ncsoccer/config"
  description = "Configuration values for NC Soccer application"
}

resource "aws_secretsmanager_secret_version" "ncsoccer" {
  secret_id = aws_secretsmanager_secret.ncsoccer.id
  secret_string = jsonencode({
    openai_api_key = var.openai_api_key
    alert_email   = var.alert_email
    environment   = var.environment
  })
}

# IAM Policy for Analysis Lambda
resource "aws_iam_role_policy" "analysis_lambda" {
  name = "ncsoccer-analysis-lambda-policy"
  role = aws_iam_role.analysis_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.app_data.arn}",
          "${aws_s3_bucket.app_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.ncsoccer.arn
        ]
      }
    ]
  })
}

# Analysis Lambda Function
resource "aws_lambda_function" "analysis" {
  function_name = local.analysis_function_name
  role          = aws_iam_role.analysis_lambda.arn
  timeout       = 300
  memory_size   = 1024
  package_type  = "Image"

  image_uri = "${aws_ecr_repository.analysis.repository_url}:${local.analysis_image_tag}"

  environment {
    variables = {
      CONFIG_SECRET   = aws_secretsmanager_secret.ncsoccer.name
      PARQUET_BUCKET = aws_s3_bucket.app_data.id
      PARQUET_KEY    = "data/parquet/data.parquet"
    }
  }

  depends_on = [aws_ecr_repository.analysis]

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "analysis" {
  name     = "ncsoccer-analysis"
  role_arn = aws_iam_role.step_function.arn

  definition = jsonencode({
    Comment = "Soccer Analysis Pipeline"
    StartAt = "ProcessQuery"
    States = {
      ProcessQuery = {
        Type = "Task"
        Resource = aws_lambda_function.analysis.arn
        Parameters = {
          "step": "query"
          "prompt.$": "$.prompt"
          "compute_iterations": 10
          "format_type.$": "$.format_type"
        }
        ResultPath = "$.result"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 2
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Next = "FormatResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next = "HandleError"
          ResultPath = "$.error"
        }]
      }
      FormatResult = {
        Type = "Task"
        Resource = aws_lambda_function.analysis.arn
        Parameters = {
          "step": "format"
          "prompt.$": "$.prompt"
          "result.$": "$.result"
          "format_type.$": "$.format_type"
        }
        ResultPath = "$.formatted_result"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 2
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Next = "ReturnSyncResponse"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next = "HandleError"
          ResultPath = "$.error"
        }]
      }
      ReturnSyncResponse = {
        Type = "Pass"
        End = true
        Parameters = {
          "statusCode": 200
          "body": {
            "result.$": "States.JsonToString($.result)",
            "formatted_result.$": "States.JsonToString($.formatted_result)"
          }
        }
      }
      HandleError = {
        Type = "Pass"
        End = true
        Parameters = {
          "statusCode": 500
          "body": {
            "error.$": "States.JsonToString($.error.Error)",
            "cause.$": "States.JsonToString($.error.Cause)"
          }
        }
      }
    }
  })
}

# IAM Role for Step Functions
resource "aws_iam_role" "step_function" {
  name = "ncsoccer-analysis-step-function-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Step Functions
resource "aws_iam_role_policy" "step_function" {
  name = "ncsoccer-analysis-step-function-policy"
  role = aws_iam_role.step_function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.analysis.arn
        ]
      }
    ]
  })
}

# Add analysis outputs
output "analysis_function_name" {
  value = aws_lambda_function.analysis.function_name
}

output "analysis_step_function_arn" {
  value = aws_sfn_state_machine.analysis.arn
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# IAM Policy for Step Function access
resource "aws_iam_user_policy" "step_function_access" {
  name = "ncsoccer-analysis-step-function-access"
  user = "mzakany"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "states:StartExecution"
        Resource = aws_sfn_state_machine.analysis.arn
      }
    ]
  })
}