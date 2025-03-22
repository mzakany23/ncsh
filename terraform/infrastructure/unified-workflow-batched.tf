###############################################################
# NC Soccer Unified Workflow - Batched - Step Function
# Implements a unified date range workflow with efficient batching
###############################################################

resource "aws_sfn_state_machine" "ncsoccer_unified_workflow_batched" {
  name     = "ncsoccer-unified-workflow-batched"
  role_arn = aws_iam_role.unified_workflow_batched_step_function_role.arn

  definition = file("${path.module}/unified-workflow-batched.asl.json")

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.step_function_batched_logs.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }

  tags = {
    Name        = "NCSoccerUnifiedWorkflowBatched"
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# CloudWatch Log Group for Step Function logs
resource "aws_cloudwatch_log_group" "step_function_batched_logs" {
  name              = "/aws/vendedlogs/states/ncsoccer-unified-workflow-batched"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# IAM Role for Step Function
resource "aws_iam_role" "unified_workflow_batched_step_function_role" {
  name = "ncsoccer_unified_workflow_batched_role"

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

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# Permissions policy for Step Function to invoke Lambda functions
resource "aws_iam_policy" "step_function_batched_lambda_policy" {
  name = "ncsoccer_step_function_batched_lambda_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ncsoccer_input_validator.arn,
          aws_lambda_function.ncsoccer_batch_planner.arn,
          aws_lambda_function.ncsoccer_batch_verifier.arn,
          aws_lambda_function.ncsoccer_scraper.arn,
          aws_lambda_function.processing.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "step_function_batched_lambda_policy_attachment" {
  role       = aws_iam_role.unified_workflow_batched_step_function_role.name
  policy_arn = aws_iam_policy.step_function_batched_lambda_policy.arn
}

###############################################################
# EventBridge Rules for the Unified Workflow with Batching
###############################################################

# Daily Scrape Schedule - trigger at 4:00 UTC daily
resource "aws_cloudwatch_event_rule" "ncsoccer_daily_unified_batched" {
  name        = "ncsoccer-daily-unified-batched"
  description = "Trigger NC Soccer unified batched workflow for current day at 04:00 UTC"

  schedule_expression = "cron(0 4 * * ? *)"
  state               = "DISABLED" # Initially disabled until fully tested

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# Monthly Scrape Schedule - trigger at 5:00 UTC on the 1st of each month
resource "aws_cloudwatch_event_rule" "ncsoccer_monthly_unified_batched" {
  name        = "ncsoccer-monthly-unified-batched"
  description = "Trigger NC Soccer unified batched workflow for entire month on the 1st day at 05:00 UTC"

  schedule_expression = "cron(0 5 1 * ? *)"
  state               = "DISABLED" # Initially disabled until fully tested

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# IAM Role for EventBridge to invoke Step Functions
resource "aws_iam_role" "unified_workflow_batched_eventbridge_role" {
  name = "ncsoccer_eventbridge_batched_step_function_role"

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

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# Policy for EventBridge to invoke Step Functions
resource "aws_iam_policy" "eventbridge_batched_step_function_policy" {
  name = "ncsoccer_eventbridge_batched_step_function_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.ncsoccer_unified_workflow_batched.arn
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "eventbridge_batched_step_function_policy_attachment" {
  role       = aws_iam_role.unified_workflow_batched_eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_batched_step_function_policy.arn
}

# Daily EventBridge Target
resource "aws_cloudwatch_event_target" "ncsoccer_daily_unified_batched_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_daily_unified_batched.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow_batched.arn
  role_arn  = aws_iam_role.unified_workflow_batched_eventbridge_role.arn

  input = jsonencode({
    start_date     = "#{aws:DateNow(YYYY)}-#{aws:DateNow(MM)}-#{aws:DateNow(DD)}",
    end_date       = "#{aws:DateNow(YYYY)}-#{aws:DateNow(MM)}-#{aws:DateNow(DD)}",
    force_scrape   = true,
    batch_size     = 1,
    bucket_name    = "ncsh-app-data"
  })
}

# Monthly EventBridge Target
resource "aws_cloudwatch_event_target" "ncsoccer_monthly_unified_batched_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_monthly_unified_batched.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow_batched.arn
  role_arn  = aws_iam_role.unified_workflow_batched_eventbridge_role.arn

  input = jsonencode({
    start_date     = "#{aws:DateNow(YYYY)}-#{aws:DateNow(MM)}-01",
    end_date       = "#{aws:DateNow(YYYY)}-#{aws:DateNow(MM)}-#{aws:DaysInMonth}",
    force_scrape   = true,
    batch_size     = 3,
    bucket_name    = "ncsh-app-data"
  })
}