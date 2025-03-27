###############################################################
# NC Soccer Lambda Function for Daily Backfill
# This Lambda will be triggered by EventBridge to run the recursive workflow
# for the last 3 days to ensure complete data
###############################################################

# Daily Backfill Lambda Function
resource "aws_lambda_function" "ncsoccer_daily_backfill" {
  function_name = "ncsoccer_daily_backfill"
  role          = aws_iam_role.lambda_utils_role.arn
  package_type  = "Image"
  timeout       = 60
  memory_size   = 256

  # This will be updated by the CI/CD pipeline
  image_uri = "${data.aws_ecr_repository.ncsoccer_utils.repository_url}:latest"

  # Use the router pattern with lambda_function.handler
  image_config {
    command = ["lambda_function.handler"]
  }

  environment {
    variables = {
      DATA_BUCKET = "ncsh-app-data"
      STATE_MACHINE_ARN = "arn:aws:states:us-east-2:552336166511:stateMachine:ncsoccer-unified-workflow-recursive"
      ARCHITECTURE_VERSION = "v2"
      FORCE_SCRAPE = "true"
      BATCH_SIZE = "1"
    }
  }
}

# CloudWatch Log Group for Daily Backfill Lambda
resource "aws_cloudwatch_log_group" "ncsoccer_daily_backfill_logs" {
  name              = "/aws/lambda/ncsoccer_daily_backfill"
  retention_in_days = 14
}
