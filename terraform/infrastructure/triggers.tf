# This file contains the original monthly schedule trigger
# These resources are now managed in monitoring.tf

/*
# EventBridge rule to trigger Step Function monthly
resource "aws_cloudwatch_event_rule" "monthly_schedule" {
  name                = "ncsoccer-monthly-schedule"
  description         = "Trigger NC Soccer scraper workflow monthly"
  schedule_expression = "cron(0 0 1 * ? *)" # Run at midnight on the first day of every month
  state              = "ENABLED"
}

# EventBridge target for Step Function
resource "aws_cloudwatch_event_target" "step_function" {
  rule      = aws_cloudwatch_event_rule.monthly_schedule.name
  target_id = "NCSoccerStepFunction"
  arn       = aws_sfn_state_machine.ncsoccer_workflow.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    year  = "$${time:getYear}"
    month = "$${time:getMonth}"
    mode  = "month"
  })
}
*/