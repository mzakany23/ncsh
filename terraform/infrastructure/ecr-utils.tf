###############################################################
# NC Soccer ECR Repository for Utility Functions
# This repository will store the Docker images for all utility Lambda functions
###############################################################

# Use data source for existing ECR repository
data "aws_ecr_repository" "ncsoccer_utils" {
  name = "ncsoccer-utils"
}

# ECR Lifecycle Policy
resource "aws_ecr_lifecycle_policy" "ncsoccer_utils" {
  repository = data.aws_ecr_repository.ncsoccer_utils.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1,
        description  = "Keep last 5 images",
        selection = {
          tagStatus   = "any",
          countType   = "imageCountMoreThan",
          countNumber = 5
        },
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Add to outputs
output "utils_ecr_repository_url" {
  description = "The URL of the ECR repository for utility functions"
  value       = data.aws_ecr_repository.ncsoccer_utils.repository_url
}

output "utils_ecr_repository_name" {
  description = "The name of the ECR repository for utility functions"
  value       = data.aws_ecr_repository.ncsoccer_utils.name
}
