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
  input = jsonencode({
    start_date           = "#{aws:DateNow(YYYY-MM-DD, -2)}", # 2 days ago
    end_date             = "#{aws:DateNow(YYYY-MM-DD)}",     # today
    force_scrape         = false,                           # Only scrape if needed
    architecture_version = "v2",
    batch_size           = 1,                               # Process one day at a time
    bucket_name          = "ncsh-app-data",
    is_sub_execution     = false
  })
}
