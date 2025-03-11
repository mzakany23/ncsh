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

# Daily Scrape EventBridge Rule
resource "aws_cloudwatch_event_rule" "daily_scrape" {
  name                = "ncsoccer-daily-scrape"
  description         = "Trigger NC Soccer scraper workflow daily"
  schedule_expression = "cron(0 4 * * ? *)" # Run at 4:00 AM UTC daily
  state               = "ENABLED"
}

# Daily Scrape EventBridge Target
resource "aws_cloudwatch_event_target" "daily_scrape_target" {
  rule      = aws_cloudwatch_event_rule.daily_scrape.name
  target_id = "NCSoccerDailyScrape"
  arn       = aws_sfn_state_machine.ncsoccer_workflow.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn

  input = jsonencode({
    year        = "#{aws:DateNow(YYYY)}"
    month       = "#{aws:DateNow(MM)}"
    day         = "#{aws:DateNow(DD)}"
    mode        = "day"
    force_scrape = true
  })
}

# Daily Process EventBridge Rule
resource "aws_cloudwatch_event_rule" "daily_process" {
  name                = "ncsoccer-daily-process"
  description         = "Trigger NC Soccer processing workflow daily"
  schedule_expression = "cron(30 4 * * ? *)" # Run at 4:30 AM UTC daily
  state               = "ENABLED"
}

# Daily Process EventBridge Target
resource "aws_cloudwatch_event_target" "daily_process_target" {
  rule      = aws_cloudwatch_event_rule.daily_process.name
  target_id = "NCSoccerDailyProcess"
  arn       = aws_sfn_state_machine.processing.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn

  input = jsonencode({
    timestamp   = "#{aws:DateNow()}"
    src_bucket  = "ncsh-app-data"
    src_prefix  = "data/json/"
    dst_bucket  = "ncsh-app-data"
    dst_prefix  = "data/parquet/"
  })
}

# Monthly Scrape EventBridge Rule
resource "aws_cloudwatch_event_rule" "monthly_scrape" {
  name                = "ncsoccer-monthly-scrape"
  description         = "Trigger NC Soccer scraper workflow for the entire month on the 1st day"
  schedule_expression = "cron(0 5 1 * ? *)" # Run at 5:00 AM UTC on the 1st day of every month
  state               = "ENABLED"
}

# Monthly Scrape EventBridge Target
resource "aws_cloudwatch_event_target" "monthly_scrape_target" {
  rule      = aws_cloudwatch_event_rule.monthly_scrape.name
  target_id = "NCSoccerMonthlyScrape"
  arn       = aws_sfn_state_machine.ncsoccer_workflow.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn

  input = jsonencode({
    year        = "#{aws:DateNow(YYYY)}"
    month       = "#{aws:DateNow(MM)}"
    mode        = "month"
    force_scrape = true
  })
}

# Monthly Process EventBridge Rule
resource "aws_cloudwatch_event_rule" "monthly_process" {
  name                = "ncsoccer-monthly-process"
  description         = "Trigger NC Soccer processing workflow after monthly scrape"
  schedule_expression = "cron(0 7 1 * ? *)" # Run at 7:00 AM UTC on the 1st day of every month
  state               = "ENABLED"
}

# Monthly Process EventBridge Target
resource "aws_cloudwatch_event_target" "monthly_process_target" {
  rule      = aws_cloudwatch_event_rule.monthly_process.name
  target_id = "NCSoccerMonthlyProcess"
  arn       = aws_sfn_state_machine.processing.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn

  input = jsonencode({
    timestamp   = "#{aws:DateNow()}"
    src_bucket  = "ncsh-app-data"
    src_prefix  = "data/json/"
    dst_bucket  = "ncsh-app-data"
    dst_prefix  = "data/parquet/"
  })
}

# CloudWatch Alarm for Scraper Step Function Failures
resource "aws_cloudwatch_metric_alarm" "scraper_failure_alarm" {
  alarm_name          = "ncsoccer-workflow-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This alarm monitors for NC Soccer scraper workflow failures"
  alarm_actions       = [aws_sns_topic.ncsoccer_alarms.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.ncsoccer_workflow.arn
  }
}

# CloudWatch Alarm for Processing Step Function Failures
resource "aws_cloudwatch_metric_alarm" "processing_failure_alarm" {
  alarm_name          = "ncsoccer-processing-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This alarm monitors for NC Soccer processing workflow failures"
  alarm_actions       = [aws_sns_topic.ncsoccer_alarms.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.processing.arn
  }
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