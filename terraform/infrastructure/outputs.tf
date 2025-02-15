# Output the app data bucket name
output "app_data_bucket" {
  description = "Name of the application data bucket"
  value       = aws_s3_bucket.app_data.id
}

# Output the ECR repository details
output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = aws_ecr_repository.ncsoccer.repository_url
}

output "ecr_repository_name" {
  description = "Name of the ECR repository"
  value       = aws_ecr_repository.ncsoccer.name
}