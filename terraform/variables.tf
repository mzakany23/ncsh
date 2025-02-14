variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "ncsoccer-scraper"
}

variable "data_bucket_name" {
  description = "Name of the S3 bucket to store scraped data"
  type        = string
  # This needs to be globally unique, so no default provided
}