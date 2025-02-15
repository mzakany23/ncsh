terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "ncsh-terraform-state"
    key    = "app/terraform.tfstate"
    region = "us-east-2"
  }
}

provider "aws" {
  region = var.aws_region
}

# ECR Repository
resource "aws_ecr_repository" "ncsoccer" {
  name                 = var.ecr_repository_name
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
          "${aws_s3_bucket.scraper_data.arn}",
          "${aws_s3_bucket.scraper_data.arn}/*"
        ]
      }
    ]
  })
}

# S3 Bucket for scraped data
resource "aws_s3_bucket" "scraper_data" {
  bucket = var.data_bucket_name
}

resource "aws_s3_bucket_versioning" "scraper_data" {
  bucket = aws_s3_bucket.scraper_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scraper_data" {
  bucket = aws_s3_bucket.scraper_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scraper_data" {
  bucket = aws_s3_bucket.scraper_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lambda Function
resource "aws_lambda_function" "ncsoccer_scraper" {
  function_name = "ncsoccer_scraper"
  role          = aws_iam_role.lambda_role.arn
  timeout       = 300 # 5 minutes
  memory_size   = 512

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ncsoccer.repository_url}:latest"

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.scraper_data.id
    }
  }
}