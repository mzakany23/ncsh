variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-2"
}

variable "openai_api_key" {
  description = "OpenAI API key for GPT-4 access"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address to receive alerts"
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
  default     = "dev"
}