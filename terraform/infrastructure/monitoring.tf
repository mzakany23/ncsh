# SNS Topic for Alarm Notifications
resource "aws_sns_topic" "ncsoccer_alarms" {
  name = "ncsoccer-alarms"
}

# SNS Subscription for Email Notifications
resource "aws_sns_topic_subscription" "email_notification" {
  topic_arn = aws_sns_topic.ncsoccer_alarms.arn
  protocol  = "email"
  endpoint  = "mzakany@gmail.com"
}

# IAM Role for EventBridge to Trigger Step Functions (used by the CloudWatch Events)
resource "aws_iam_role" "eventbridge_step_function_role" {
  name = "EventBridgeStepFunctionExecutionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for EventBridge to Trigger Step Functions
resource "aws_iam_role_policy" "eventbridge_step_function_policy" {
  name = "eventbridge-step-function-policy"
  role = aws_iam_role.eventbridge_step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:ncsoccer-*"
        ]
      }
    ]
  })
}

# CloudWatch Alarm for Scraper Lambda Errors
resource "aws_cloudwatch_metric_alarm" "scraper_lambda_error_alarm" {
  alarm_name          = "ncsoccer-scraper-lambda-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This alarm monitors for NC Soccer scraper lambda errors"
  alarm_actions       = [aws_sns_topic.ncsoccer_alarms.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.ncsoccer_scraper.function_name
  }
}

# CloudWatch Alarm for Processor Lambda Errors
resource "aws_cloudwatch_metric_alarm" "processing_lambda_error_alarm" {
  alarm_name          = "ncsoccer-processing-lambda-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This alarm monitors for NC Soccer processing lambda errors"
  alarm_actions       = [aws_sns_topic.ncsoccer_alarms.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.processing.function_name
  }
}