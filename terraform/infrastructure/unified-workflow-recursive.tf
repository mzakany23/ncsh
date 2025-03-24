###############################################################
# NC Soccer Unified Workflow - Recursive - Step Function
# Implements a unified date range workflow with recursive execution for large date ranges
###############################################################

resource "aws_sfn_state_machine" "ncsoccer_unified_workflow_recursive" {
  name     = "ncsoccer-unified-workflow-recursive"
  role_arn = aws_iam_role.unified_workflow_recursive_step_function_role.arn

  definition = file("${path.module}/unified-workflow-recursive.asl.json")

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.step_function_recursive_logs.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.step_function_recursive_lambda_policy_attachment,
    aws_cloudwatch_log_group.step_function_recursive_logs
  ]

  tags = {
    Name        = "NCSoccerUnifiedWorkflowRecursive"
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# CloudWatch Log Group for Step Function logs
resource "aws_cloudwatch_log_group" "step_function_recursive_logs" {
  name              = "/aws/vendedlogs/states/ncsoccer-unified-workflow-recursive"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# IAM Role for Step Function
resource "aws_iam_role" "unified_workflow_recursive_step_function_role" {
  name = "ncsoccer_unified_workflow_recursive_role"

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
resource "aws_iam_policy" "unified_workflow_recursive_step_function_policy" {
  name = "ncsoccer_unified_workflow_recursive_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_input_validator",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_batch_planner",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_scraper",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_batch_verifier",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_date_range_splitter",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer_execution_checker",
          "arn:aws:lambda:us-east-2:552336166511:function:ncsoccer-processing"
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

# Attach IAM Policy to Role
resource "aws_iam_role_policy_attachment" "step_function_recursive_lambda_policy_attachment" {
  role       = aws_iam_role.unified_workflow_recursive_step_function_role.name
  policy_arn = aws_iam_policy.unified_workflow_recursive_step_function_policy.arn
}

# EventBridge Rule to trigger Step Function on schedule (for testing)
resource "aws_cloudwatch_event_rule" "ncsoccer_unified_workflow_recursive_daily_test" {
  name                = "ncsoccer-unified-workflow-recursive-daily-test"
  description         = "Trigger NC Soccer Recursive Workflow Daily Test"
  schedule_expression = "cron(0 8 * * ? *)" # 8:00 AM UTC every day
  is_enabled          = false               # Disabled by default, enable for testing

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# EventBridge Target for Step Function
resource "aws_cloudwatch_event_target" "ncsoccer_unified_workflow_recursive_daily_test_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_unified_workflow_recursive_daily_test.name
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow_recursive.arn
  role_arn  = aws_iam_role.unified_workflow_recursive_eventbridge_role.arn

  input = jsonencode({
    start_date           = "2023-01-01",
    end_date             = "2023-01-02",
    force_scrape         = false,
    architecture_version = "v2",
    batch_size           = 3,
    bucket_name          = "ncsh-app-data"
  })
}

# IAM Role for EventBridge to trigger Step Function
resource "aws_iam_role" "unified_workflow_recursive_eventbridge_role" {
  name = "ncsoccer_eventbridge_recursive_step_function_role"

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

# IAM Policy for EventBridge to trigger Step Function
resource "aws_iam_policy" "unified_workflow_recursive_eventbridge_policy" {
  name = "ncsoccer_eventbridge_recursive_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.ncsoccer_unified_workflow_recursive.arn
        ]
      }
    ]
  })
}

# Attach IAM Policy to Role
resource "aws_iam_role_policy_attachment" "unified_workflow_recursive_eventbridge_policy_attachment" {
  role       = aws_iam_role.unified_workflow_recursive_eventbridge_role.name
  policy_arn = aws_iam_policy.unified_workflow_recursive_eventbridge_policy.arn
}
