###############################################################
# NC Soccer Lambda Functions for Batched Workflow
# Defines new Lambda functions for the unified date range workflow
###############################################################

# ECR Repository for utility Lambda functions
resource "aws_ecr_repository" "ncsoccer_utils" {
  name                 = "ncsoccer-utils"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# ECR Lifecycle Policy
resource "aws_ecr_lifecycle_policy" "ncsoccer_utils" {
  repository = aws_ecr_repository.ncsoccer_utils.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus     = "any"
          countType     = "imageCountMoreThan"
          countNumber   = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ECR Repository Policy for Lambda Access
resource "aws_ecr_repository_policy" "ncsoccer_utils_policy" {
  repository = aws_ecr_repository.ncsoccer_utils.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowLambdaServiceAccess"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ]
      }
    ]
  })
}

# IAM Role for Utility Lambda Functions
resource "aws_iam_role" "lambda_utils_role" {
  name = "ncsoccer_lambda_utils_role"

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

# IAM Policy for Utility Lambda Functions
resource "aws_iam_role_policy" "lambda_utils_policy" {
  name = "ncsoccer_lambda_utils_policy"
  role = aws_iam_role.lambda_utils_role.id

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
          "s3:GetObjectVersion"
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
        Resource = [aws_ecr_repository.ncsoccer_utils.arn]
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

# Input Validator Lambda
resource "aws_lambda_function" "ncsoccer_input_validator" {
  function_name = "ncsoccer_input_validator"
  role          = aws_iam_role.lambda_utils_role.arn
  timeout       = 10
  memory_size   = 128

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.app_data.id
    }
  }

  # Ignore image_uri changes since they are managed by CI/CD
  lifecycle {
    ignore_changes = [image_uri]
  }
}

# Batch Planner Lambda
resource "aws_lambda_function" "ncsoccer_batch_planner" {
  function_name = "ncsoccer_batch_planner"
  role          = aws_iam_role.lambda_utils_role.arn
  timeout       = 10
  memory_size   = 128

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.app_data.id
    }
  }

  # Ignore image_uri changes since they are managed by CI/CD
  lifecycle {
    ignore_changes = [image_uri]
  }
}

# Batch Verifier Lambda
resource "aws_lambda_function" "ncsoccer_batch_verifier" {
  function_name = "ncsoccer_batch_verifier"
  role          = aws_iam_role.lambda_utils_role.arn
  timeout       = 10
  memory_size   = 128

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.app_data.id
    }
  }

  # Ignore image_uri changes since they are managed by CI/CD
  lifecycle {
    ignore_changes = [image_uri]
  }
}

# CloudWatch Log Groups for Lambda functions
resource "aws_cloudwatch_log_group" "ncsoccer_input_validator_logs" {
  name              = "/aws/lambda/ncsoccer_input_validator"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

resource "aws_cloudwatch_log_group" "ncsoccer_batch_planner_logs" {
  name              = "/aws/lambda/ncsoccer_batch_planner"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

resource "aws_cloudwatch_log_group" "ncsoccer_batch_verifier_logs" {
  name              = "/aws/lambda/ncsoccer_batch_verifier"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}