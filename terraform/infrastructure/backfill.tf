# Note: The backfill state machine has been consolidated into the unified workflow
# and is now managed in unified-workflow.tf

# EventBridge Rule to manually trigger the backfill job (disabled by default)
resource "aws_cloudwatch_event_rule" "backfill_trigger" {
  name                = "ncsoccer-backfill-trigger"
  description         = "Manual trigger for backfill job (disabled by default)"
  schedule_expression = "cron(0 1 1 1 ? 2099)" # Far future date, effectively disabled
  state               = "DISABLED"
}

# EventBridge Target for the backfill job - now using the unified workflow
resource "aws_cloudwatch_event_target" "backfill_target" {
  rule      = aws_cloudwatch_event_rule.backfill_trigger.name
  target_id = "BackfillStepFunction"
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow.arn
  role_arn  = aws_iam_role.unified_workflow_eventbridge_role.arn

  input = jsonencode({
    "operation": "backfill",
    "parameters": {
      "startDate": "2007-01-01",
      "endDate": "2025-12-31",
      "useNewProcessingCode": true
    }
  })
}

# Monitoring for the backfill job - now monitoring the unified workflow
resource "aws_cloudwatch_metric_alarm" "backfill_failure_alarm" {
  alarm_name          = "ncsoccer-backfill-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "This metric monitors backfill failures in the unified workflow"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.ncsoccer_unified_workflow.arn
  }

  alarm_actions = [aws_sns_topic.ncsoccer_alarms.arn]
  ok_actions    = [aws_sns_topic.ncsoccer_alarms.arn]
}

# Log group specifically for backfill executions
resource "aws_cloudwatch_log_group" "backfill_logs" {
  name              = "/aws/states/ncsoccer-backfill"
  retention_in_days = 30

  tags = {
    Application = "ncsoccer"
    Component   = "backfill"
  }
}

# Lambda function for efficient backfill processing
resource "aws_lambda_function" "ncsoccer_backfill" {
  function_name    = "ncsoccer_backfill"
  image_uri        = "${aws_ecr_repository.ncsoccer.repository_url}:latest"
  package_type     = "Image"
  role             = aws_iam_role.lambda_role.arn
  memory_size      = 1024
  timeout          = 900  # 15 minutes for long-running backfill process

  environment {
    variables = {
      DATA_BUCKET    = aws_s3_bucket.app_data.bucket
      DYNAMODB_TABLE = "ncsh-scraped-dates"
      BACKFILL_MODE  = "true"
    }
  }

  ephemeral_storage {
    size = 10240  # 10GB of ephemeral storage
  }

  tags = {
    Name        = "ncsoccer_backfill"
    Application = "ncsoccer"
    Component   = "backfill"
  }

  # Ignore image_uri changes since they are managed by CI/CD
  lifecycle {
    ignore_changes = [image_uri]
  }
}

# Log group for backfill Lambda
resource "aws_cloudwatch_log_group" "backfill_lambda_logs" {
  name              = "/aws/lambda/ncsoccer_backfill"
  retention_in_days = 30

  tags = {
    Application = "ncsoccer"
    Component   = "backfill"
  }
}