# ECR Repository for the processing Lambda
resource "aws_ecr_repository" "processing" {
  name                 = "ncsoccer-processing"
  image_tag_mutability = "MUTABLE"
}

# ECR Lifecycle Policy
resource "aws_ecr_lifecycle_policy" "processing" {
  repository = aws_ecr_repository.processing.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ECR Repository Policy for Lambda Access
resource "aws_ecr_repository_policy" "processing" {
  repository = aws_ecr_repository.processing.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LambdaECRImageRetrievalPolicy"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
      }
    ]
  })
}

# IAM role for the processing Lambda
resource "aws_iam_role" "processing_lambda" {
  name = "ncsoccer-processing-lambda"

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

# IAM policy for the processing Lambda
resource "aws_iam_role_policy" "processing_lambda" {
  name = "ncsoccer-processing-lambda"
  role = aws_iam_role.processing_lambda.id

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
          "arn:aws:s3:::ncsh-app-data",
          "arn:aws:s3:::ncsh-app-data/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan",
          "dynamodb:Query"
        ]
        Resource = aws_dynamodb_table.scraped_dates.arn
      }
    ]
  })
}

# IAM role for the Step Function
resource "aws_iam_role" "processing_step_function" {
  name = "ncsoccer-processing-step-function"

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

# IAM policy for the Step Function
resource "aws_iam_role_policy" "processing_step_function" {
  name = "ncsoccer-processing-step-function"
  role = aws_iam_role.processing_step_function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.processing.arn
      }
    ]
  })
}

# Lambda function for JSON to Parquet conversion
resource "aws_lambda_function" "processing" {
  function_name = "ncsoccer-processing"
  role          = aws_iam_role.processing_lambda.arn
  timeout       = 900
  memory_size   = 512

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.processing.repository_url}:latest"
}

# State Machine for processing workflow
resource "aws_sfn_state_machine" "processing" {
  name     = "ncsoccer-processing"
  role_arn = aws_iam_role.processing_step_function.arn

  definition = jsonencode({
    Comment = "NC Soccer Data Processing Pipeline",
    StartAt = "ListJSONFiles",
    States = {
      ListJSONFiles = {
        Type = "Task",
        Resource = aws_lambda_function.processing.arn,
        Parameters = {
          "operation": "list_files",
          "src_bucket.$": "$.src_bucket",
          "src_prefix.$": "$.src_prefix"
        },
        Next = "ProcessJSONToParquet",
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next = "HandleError"
        }]
      },
      ProcessJSONToParquet = {
        Type = "Task",
        Resource = aws_lambda_function.processing.arn,
        Parameters = {
          "operation": "convert",
          "src_bucket.$": "$.src_bucket",
          "src_prefix.$": "$.src_prefix",
          "dst_bucket.$": "$.dst_bucket",
          "dst_prefix.$": "$.dst_prefix",
          "files.$": "$.files"
        },
        Next = "Success",
        Catch = [{
          ErrorEquals = ["States.ALL"],
          Next = "HandleError"
        }]
      },
      HandleError = {
        Type = "Pass",
        Result = {
          "status": "FAILED",
          "error.$": "$.error",
          "cause.$": "$.cause"
        },
        End = true
      },
      Success = {
        Type = "Pass",
        Result = {
          "status": "SUCCESS",
          "message": "Successfully converted JSON files to Parquet",
          "timestamp.$": "$$.Execution.StartTime"
        },
        End = true
      }
    }
  })
}