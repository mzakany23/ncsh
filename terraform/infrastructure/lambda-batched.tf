###############################################################
# NC Soccer Lambda Functions for Batched Workflow
# Defines new Lambda functions for the unified date range workflow
###############################################################

###############################################################
# ECR Repository for Utility Functions
###############################################################

# Using data source for existing ECR repository
data "aws_ecr_repository" "ncsoccer_utils" {
  name = "ncsoccer-utils"
}

# ECR Lifecycle Policy
resource "aws_ecr_lifecycle_policy" "ncsoccer_utils" {
  repository = data.aws_ecr_repository.ncsoccer_utils.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1,
        description  = "Keep last 5 images",
        selection = {
          tagStatus   = "any",
          countType   = "imageCountMoreThan",
          countNumber = 5
        },
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ECR Repository Policy
resource "aws_ecr_repository_policy" "ncsoccer_utils_policy" {
  repository = data.aws_ecr_repository.ncsoccer_utils.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "AllowLambdaServiceAccess",
        Effect    = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ]
      }
    ]
  })
}

###############################################################
# IAM Role for Utility Lambda Functions
###############################################################

resource "aws_iam_role" "lambda_utils_role" {
  name = "ncsoccer_lambda_utils_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_utils_policy" {
  name = "ncsoccer_lambda_utils_policy"
  role = aws_iam_role.lambda_utils_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::ncsh-app-data",
          "arn:aws:s3:::ncsh-app-data/*"
        ]
      }
    ]
  })
}

###############################################################
# Lambda Functions for Unified Workflow with Batching
###############################################################

# Input Validator Lambda
resource "aws_lambda_function" "ncsoccer_input_validator" {
  function_name = "ncsoccer_input_validator"
  role          = aws_iam_role.lambda_utils_role.arn
  package_type  = "Image"
  timeout       = 10
  memory_size   = 128

  # This will be updated by the CI/CD pipeline
  image_uri = "${data.aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = "ncsh-app-data"
    }
  }
}

# Batch Planner Lambda
resource "aws_lambda_function" "ncsoccer_batch_planner" {
  function_name = "ncsoccer_batch_planner"
  role          = aws_iam_role.lambda_utils_role.arn
  package_type  = "Image"
  timeout       = 10
  memory_size   = 128

  # This will be updated by the CI/CD pipeline
  image_uri = "${data.aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = "ncsh-app-data"
    }
  }
}

# Batch Verifier Lambda
resource "aws_lambda_function" "ncsoccer_batch_verifier" {
  function_name = "ncsoccer_batch_verifier"
  role          = aws_iam_role.lambda_utils_role.arn
  package_type  = "Image"
  timeout       = 60
  memory_size   = 256

  # This will be updated by the CI/CD pipeline
  image_uri = "${data.aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = "ncsh-app-data"
    }
  }
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "ncsoccer_input_validator_logs" {
  name              = "/aws/lambda/${aws_lambda_function.ncsoccer_input_validator.function_name}"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

resource "aws_cloudwatch_log_group" "ncsoccer_batch_planner_logs" {
  name              = "/aws/lambda/${aws_lambda_function.ncsoccer_batch_planner.function_name}"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

resource "aws_cloudwatch_log_group" "ncsoccer_batch_verifier_logs" {
  name              = "/aws/lambda/${aws_lambda_function.ncsoccer_batch_verifier.function_name}"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}