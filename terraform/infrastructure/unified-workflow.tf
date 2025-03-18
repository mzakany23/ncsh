###############################################################
# NC Soccer Unified Workflow - Step Function
# Integrates scraping and processing for daily, monthly and backfill
###############################################################

resource "aws_sfn_state_machine" "ncsoccer_unified_workflow" {
  name     = "ncsoccer-unified-workflow"
  role_arn = aws_iam_role.unified_workflow_step_function_role.arn

  definition = file("${path.module}/unified-workflow-updated.asl.json")

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }

  tags = {
    Name        = "NCSoccerUnifiedWorkflow"
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# CloudWatch Log Group for Step Function logs
resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/vendedlogs/states/ncsoccer-unified-workflow"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# IAM Role for Step Function
resource "aws_iam_role" "unified_workflow_step_function_role" {
  name = "ncsoccer_unified_workflow_role"

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
resource "aws_iam_policy" "step_function_lambda_policy" {
  name = "ncsoccer_step_function_lambda_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.ncsoccer_scraper.arn,
          aws_lambda_function.processing.arn,
          aws_lambda_function.ncsoccer_backfill.arn
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
resource "aws_iam_role_policy_attachment" "step_function_lambda_policy_attachment" {
  role       = aws_iam_role.unified_workflow_step_function_role.name
  policy_arn = aws_iam_policy.step_function_lambda_policy.arn
}

###############################################################
# EventBridge Rules for the Unified Workflow
###############################################################

# Daily Scrape Schedule - trigger at 4:00 UTC daily
resource "aws_cloudwatch_event_rule" "ncsoccer_daily_unified" {
  name        = "ncsoccer-daily-unified"
  description = "Trigger NC Soccer unified workflow for current day at 04:00 UTC"

  schedule_expression = "cron(0 4 * * ? *)"
  state               = "ENABLED"

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# Monthly Scrape Schedule - trigger at 5:00 UTC on the 1st of each month
resource "aws_cloudwatch_event_rule" "ncsoccer_monthly_unified" {
  name        = "ncsoccer-monthly-unified"
  description = "Trigger NC Soccer unified workflow for entire month on the 1st day at 05:00 UTC"

  schedule_expression = "cron(0 5 1 * ? *)"
  state               = "ENABLED"

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# Backfill Trigger Schedule - DISABLED by default, manual trigger only
resource "aws_cloudwatch_event_rule" "ncsoccer_backfill_unified" {
  name        = "ncsoccer-backfill-unified"
  description = "Manual trigger for NC Soccer backfill workflow (disabled by default)"

  schedule_expression = "cron(0 1 1 1 ? 2099)" # Far future date to effectively disable automatic triggering
  state               = "DISABLED"

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# IAM Role for EventBridge to invoke Step Functions
resource "aws_iam_role" "unified_workflow_eventbridge_role" {
  name = "ncsoccer_eventbridge_step_function_role"

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
resource "aws_iam_policy" "eventbridge_step_function_policy" {
  name = "ncsoccer_eventbridge_step_function_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.ncsoccer_unified_workflow.arn
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "eventbridge_step_function_policy_attachment" {
  role       = aws_iam_role.unified_workflow_eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_step_function_policy.arn
}

# Daily EventBridge Target
resource "aws_cloudwatch_event_target" "ncsoccer_daily_unified_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_daily_unified.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow.arn
  role_arn  = aws_iam_role.unified_workflow_eventbridge_role.arn

  input = jsonencode({
    operation = "daily",
    parameters = {
      day         = "#{aws:DateNow(DD)}",
      month       = "#{aws:DateNow(MM)}",
      year        = "#{aws:DateNow(YYYY)}",
      force_scrape = true
    }
  })
}

# Monthly EventBridge Target
resource "aws_cloudwatch_event_target" "ncsoccer_monthly_unified_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_monthly_unified.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow.arn
  role_arn  = aws_iam_role.unified_workflow_eventbridge_role.arn

  input = jsonencode({
    operation = "monthly",
    parameters = {
      month       = "#{aws:DateNow(MM)}",
      year        = "#{aws:DateNow(YYYY)}",
      force_scrape = true
    }
  })
}

# Backfill EventBridge Target
resource "aws_cloudwatch_event_target" "ncsoccer_backfill_unified_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_backfill_unified.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow.arn
  role_arn  = aws_iam_role.unified_workflow_eventbridge_role.arn

  input = jsonencode({
    operation = "backfill",
    parameters = {
      startDate   = "2010-01-01",
      endDate     = "#{aws:DateNow(YYYY)}-#{aws:DateNow(MM)}-#{aws:DateNow(DD)}"
    }
  })
}
