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

# Get current AWS account ID
data "aws_caller_identity" "current" {}