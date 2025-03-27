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

# EventBridge Target for Daily Backfill Lambda
resource "aws_cloudwatch_event_target" "ncsoccer_recursive_daily_backfill_target" {
  rule      = aws_cloudwatch_event_rule.ncsoccer_recursive_daily_backfill.name
  target_id = "NCSoccerRecursiveDailyBackfill"
  arn       = aws_lambda_function.ncsoccer_daily_backfill.arn
}
