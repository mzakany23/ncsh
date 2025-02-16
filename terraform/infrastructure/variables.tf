variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-2"
}

variable "alert_email" {
  description = "Email address to receive budget alerts"
  type        = string
}