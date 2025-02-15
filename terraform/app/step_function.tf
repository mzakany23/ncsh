# IAM Role for Step Function
resource "aws_iam_role" "step_function_role" {
  name = "ncsoccer_step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Step Function
resource "aws_iam_role_policy" "step_function_policy" {
  name = "ncsoccer_step_function_policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ncsoccer_scraper.arn
        ]
      }
    ]
  })
}

# Step Function Definition
resource "aws_sfn_state_machine" "ncsoccer_workflow" {
  name     = "ncsoccer-workflow"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "NC Soccer Scraper Workflow"
    StartAt = "ScrapeSchedule"
    States = {
      ScrapeSchedule = {
        Type = "Task"
        Resource = aws_lambda_function.ncsoccer_scraper.arn
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "HandleError"
          }
        ]
        End = true
      }
      HandleError = {
        Type = "Pass"
        Result = {
          error = "Failed to scrape schedule"
          status = "FAILED"
        }
        End = true
      }
    }
  })
}

# EventBridge Rule for Monthly Schedule
resource "aws_cloudwatch_event_rule" "monthly_schedule" {
  name                = "ncsoccer-monthly-schedule"
  description         = "Trigger NC Soccer scraper monthly"
  schedule_expression = "cron(0 0 1 * ? *)" # Run at midnight on the 1st of every month

  is_enabled = true
}

resource "aws_cloudwatch_event_target" "step_function" {
  rule      = aws_cloudwatch_event_rule.monthly_schedule.name
  target_id = "TriggerStepFunction"
  arn       = aws_sfn_state_machine.ncsoccer_workflow.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    year = "$.year"
    month = "$.month"
    mode = "month"
    force_rescrape = false
  })
}

# IAM Role for EventBridge
resource "aws_iam_role" "eventbridge_role" {
  name = "ncsoccer_eventbridge_role"

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

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "ncsoccer_eventbridge_policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.ncsoccer_workflow.arn
        ]
      }
    ]
  })
}