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

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.ncsoccer_scraper.function_name
}

output "step_function_arn" {
  description = "ARN of the Step Function state machine"
  value       = aws_sfn_state_machine.ncsoccer_workflow.arn
}