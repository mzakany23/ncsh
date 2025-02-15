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

variable "github_repo" {
  description = "GitHub repository in format owner/repo"
  type        = string
  # Example: "username/ncsoccer"
}

variable "tf_state_bucket" {
  description = "Name of the S3 bucket for Terraform state"
  type        = string
  # This needs to be globally unique, so no default provided
}