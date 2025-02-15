terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "ncsh-terraform-state"
    key            = "infrastructure/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "ncsh-terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for Application Data
resource "aws_s3_bucket" "app_data" {
  bucket = "ncsh-app-data"
}

resource "aws_s3_bucket_versioning" "app_data" {
  bucket = aws_s3_bucket.app_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "app_data" {
  bucket = aws_s3_bucket.app_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ECR Repository
resource "aws_ecr_repository" "ncsoccer" {
  name                 = "ncsoccer-scraper"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "ncsoccer_lambda_role"

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

# IAM Policy for Lambda
resource "aws_iam_role_policy" "lambda_policy" {
  name = "ncsoccer_lambda_policy"
  role = aws_iam_role.lambda_role.id

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
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:PutObjectAcl",
          "s3:GetObjectAcl"
        ]
        Resource = [
          aws_s3_bucket.app_data.arn,
          "${aws_s3_bucket.app_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ]
        Resource = [aws_ecr_repository.ncsoccer.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "ncsoccer_scraper" {
  function_name = "ncsoccer_scraper"
  role          = aws_iam_role.lambda_role.arn
  timeout       = 300 # 5 minutes
  memory_size   = 512

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.ncsoccer.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.app_data.id
    }
  }
}

# IAM Role for Step Function
resource "aws_iam_role" "step_function_role" {
  name = "ncsoccer_step_function_role"

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

# IAM Policy for Step Function
resource "aws_iam_role_policy" "step_function_policy" {
  name = "ncsoccer_step_function_policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ncsoccer_scraper.arn
        ]
      }
    ]
  })
}

# Step Function Definition
resource "aws_sfn_state_machine" "ncsoccer_workflow" {
  name     = "ncsoccer-workflow"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "NC Soccer Scraper Workflow"
    StartAt = "ScrapeSchedule"
    States = {
      ScrapeSchedule = {
        Type = "Task"
        Resource = aws_lambda_function.ncsoccer_scraper.arn
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "HandleError"
          }
        ]
        End = true
      }
      HandleError = {
        Type = "Pass"
        Result = {
          error = "Failed to scrape schedule"
          status = "FAILED"
        }
        End = true
      }
    }
  })
}

# EventBridge rule to trigger Step Function monthly
resource "aws_cloudwatch_event_rule" "monthly_schedule" {
  name                = "ncsoccer-monthly-schedule"
  description         = "Trigger NC Soccer scraper workflow monthly"
  schedule_expression = "cron(0 0 1 * ? *)" # Run at midnight on the first day of every month
  state              = "ENABLED"
}

# EventBridge target for Step Function
resource "aws_cloudwatch_event_target" "step_function" {
  rule      = aws_cloudwatch_event_rule.monthly_schedule.name
  target_id = "NCSoccerStepFunction"
  arn       = aws_sfn_state_machine.ncsoccer_workflow.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    year  = "$${time:getYear}"
    month = "$${time:getMonth}"
    mode  = "month"
  })
}

# IAM Role for EventBridge
resource "aws_iam_role" "eventbridge_role" {
  name = "ncsoccer_eventbridge_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for EventBridge
resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "ncsoccer_eventbridge_policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.ncsoccer_workflow.arn
        ]
      }
    ]
  })
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}