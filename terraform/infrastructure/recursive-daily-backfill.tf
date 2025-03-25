# Daily Backfill EventBridge Rule using Recursive Workflow
# This rule runs daily and processes the last 3 days to ensure complete data

resource "aws_cloudwatch_event_rule" "ncsoccer_recursive_daily_backfill" {
  name                = "ncsoccer-recursive-daily-backfill"
  description         = "Trigger NC Soccer Recursive Workflow to process the last 3 days"
  schedule_expression = "cron(0 6 * * ? *)" # 6:00 AM UTC every day
  state               = "ENABLED"

  tags = {
    Environment = var.environment
    Project     = "ncsoccer"
  }
}

# EventBridge Target for Recursive Workflow
resource "aws_cloudwatch_event_target" "ncsoccer_recursive_daily_backfill_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_recursive_daily_backfill.name
  target_id = "NCSoccerRecursiveDailyBackfill"
  arn       = aws_sfn_state_machine.ncsoccer_unified_workflow_recursive.arn
  role_arn  = aws_iam_role.unified_workflow_recursive_eventbridge_role.arn

  # Calculate the date range dynamically: today and the previous 2 days
  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
{
  "start_date": "$${time:0:4}-$${time:5:2}-$${time:8:2|-2}",
  "end_date": "$${time:0:10}",
  "force_scrape": false,
  "batch_size": 1,
  "bucket_name": "ncsh-app-data",
  "architecture_version": "v2",
  "is_sub_execution": false
}
EOF
  }
}
