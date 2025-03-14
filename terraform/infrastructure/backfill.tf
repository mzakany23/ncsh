# State Machine for backfilling historical data with configurable range
resource "aws_sfn_state_machine" "backfill_state_machine" {
  name     = "ncsoccer-backfill"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "Backfill historical data from 2007 to present"
    StartAt = "Initialize"

    States = {
      "Initialize" = {
        Type = "Pass",
        Result = {
          current_year = 2010,
          current_month = 1,
          count = 0,
          stats = {
            processed = 0,
            failed = 0
          }
        },
        ResultPath = "$.iterator",
        Next = "CheckIteration"
      },

      "CheckIteration" = {
        Type = "Choice",
        Choices = [
          {
            And = [
              {
                Variable = "$.iterator.current_year",
                NumericGreaterThan = 2025
              },
              {
                Variable = "$.iterator.current_month",
                NumericGreaterThan = 1
              }
            ],
            Next = "FinalProcessing"
          }
        ],
        Default = "ScrapeMonth"
      },

      "ScrapeMonth" = {
        Type = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          # Invoke the backfill Lambda with 15 minute timeout
          FunctionName = aws_lambda_function.ncsoccer_backfill.function_name,
          Payload = {
            # Backfill specific range - will resume from checkpoint if interrupted
            "start_year": 2007,
            "start_month": 1,
            "end_year": 2025,
            "end_month": 12,
            "force_scrape": false
          }
        },
        ResultPath = "$.ScrapeResult",
        Retry = [
          {
            ErrorEquals = ["States.ALL"],
            IntervalSeconds = 30,
            MaxAttempts = 2,
            BackoffRate = 2.0
          }
        ],
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath = "$.error",
            Next = "RecordFailure"
          }
        ],
        Next = "ListFiles"
      },

      "ListFiles" = {
        Type = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.processing.function_name,
          Payload = {
            "operation": "list_files",
            "src_bucket": "ncsh-app-data",
            "src_prefix": "data/json/",
            "dst_bucket": "ncsh-app-data",
            "dst_prefix": "data/parquet/"
          }
        },
        ResultSelector = {
          "files.$": "$.Payload.files",
          "src_bucket.$": "$.Payload.src_bucket", 
          "src_prefix.$": "$.Payload.src_prefix",
          "dst_bucket.$": "$.Payload.dst_bucket",
          "dst_prefix.$": "$.Payload.dst_prefix"
        },
        ResultPath = "$.FilesList",
        Retry = [
          {
            ErrorEquals = ["States.ALL"],
            IntervalSeconds = 30,
            MaxAttempts = 2,
            BackoffRate = 2.0
          }
        ],
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath = "$.error",
            Next = "RecordFailure"
          }
        ],
        Next = "CheckFilesExist"
      },

      "CheckFilesExist" = {
        Type = "Choice",
        Choices = [
          {
            And: [
              {
                Variable: "$.FilesList.files",
                IsPresent: true
              },
              {
                Variable: "$.FilesList.files[0]",
                IsPresent: true
              }
            ],
            Next = "ProcessMonth"
          }
        ],
        Default = "RecordNoFiles"
      },
      
      "RecordNoFiles" = {
        Type = "Pass",
        Parameters = {
          "iterator": {
            "current_year.$": "$.iterator.current_year",
            "current_month.$": "$.iterator.current_month",
            "count.$": "States.MathAdd($.iterator.count, 1)",
            "stats": {
              "processed.$": "$.iterator.stats.processed",
              "failed.$": "States.MathAdd($.iterator.stats.failed, 1)"
            }
          },
          "FilesList.$": "$.FilesList",
          "ScrapeResult.$": "$.ScrapeResult",
          "message": "No files found to process"
        },
        ResultPath = "$",
        Next = "Wait"
      },

      "ProcessMonth" = {
        Type = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.processing.function_name,
          Payload = {
            "operation": "convert",
            "files.$": "$.FilesList.files",
            "src_bucket.$": "$.FilesList.src_bucket",
            "src_prefix.$": "$.FilesList.src_prefix",
            "dst_bucket.$": "$.FilesList.dst_bucket",
            "dst_prefix.$": "$.FilesList.dst_prefix"
          }
        },
        ResultPath = "$.ProcessingResult",
        Retry = [
          {
            ErrorEquals = ["States.ALL"],
            IntervalSeconds = 30,
            MaxAttempts = 2,
            BackoffRate = 2.0
          }
        ],
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath = "$.error",
            Next = "RecordFailure"
          }
        ],
        Next = "RecordSuccess"
      },

      "RecordSuccess" = {
        Type = "Pass",
        Parameters = {
          "iterator": {
            "current_year.$": "$.iterator.current_year",
            "current_month.$": "$.iterator.current_month",
            "count.$": "States.MathAdd($.iterator.count, 1)",
            "stats": {
              "processed.$": "States.MathAdd($.iterator.stats.processed, 1)",
              "failed.$": "$.iterator.stats.failed"
            }
          },
          "FilesList.$": "$.FilesList",
          "ProcessingResult.$": "$.ProcessingResult",
          "ScrapeResult.$": "$.ScrapeResult"
        },
        ResultPath = "$",
        Next = "Wait"
      },

      "RecordFailure" = {
        Type = "Pass",
        Parameters = {
          "iterator": {
            "current_year.$": "$.iterator.current_year",
            "current_month.$": "$.iterator.current_month",
            "count.$": "States.MathAdd($.iterator.count, 1)",
            "stats": {
              "processed.$": "$.iterator.stats.processed",
              "failed.$": "States.MathAdd($.iterator.stats.failed, 1)"
            }
          },
          "error.$": "$.error",
          "FilesList.$": "$.FilesList",
          "ScrapeResult.$": "$.ScrapeResult"
        },
        ResultPath = "$",
        Next = "Wait"
      },

      "Wait" = {
        Type = "Wait",
        Seconds = 10,
        Next = "UpdateMonth"
      },

      "UpdateMonth" = {
        Type = "Choice",
        Choices = [
          {
            Variable = "$.iterator.current_month",
            NumericEquals = 12,
            Next = "IncrementYear"
          }
        ],
        Default = "IncrementMonth"
      },

      "IncrementMonth" = {
        Type = "Pass",
        Parameters = {
          "iterator": {
            "current_year.$": "$.iterator.current_year",
            "current_month.$": "States.MathAdd($.iterator.current_month, 1)",
            "count.$": "$.iterator.count",
            "stats.$": "$.iterator.stats"
          }
        },
        ResultPath = "$",
        Next = "CheckIteration"
      },

      "IncrementYear" = {
        Type = "Pass",
        Parameters = {
          "iterator": {
            "current_year.$": "States.MathAdd($.iterator.current_year, 1)",
            "current_month": 1,
            "count.$": "$.iterator.count",
            "stats.$": "$.iterator.stats"
          }
        },
        ResultPath = "$",
        Next = "CheckIteration"
      },

      "FinalProcessing" = {
        Type = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.processing.function_name,
          Payload = {
            "operation": "list_files",
            "src_bucket": "ncsh-app-data",
            "src_prefix": "data/json/",
            "dst_bucket": "ncsh-app-data",
            "dst_prefix": "data/parquet/"
          }
        },
        ResultSelector = {
          "files.$": "$.Payload.files",
          "src_bucket.$": "$.Payload.src_bucket", 
          "src_prefix.$": "$.Payload.src_prefix",
          "dst_bucket.$": "$.Payload.dst_bucket",
          "dst_prefix.$": "$.Payload.dst_prefix"
        },
        ResultPath = "$.FinalFilesList",
        Next = "PerformFinalProcessing"
      },
      
      "PerformFinalProcessing" = {
        Type = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.processing.function_name,
          Payload = {
            "operation": "convert",
            "files.$": "$.FinalFilesList.files", 
            "src_bucket.$": "$.FinalFilesList.src_bucket",
            "src_prefix.$": "$.FinalFilesList.src_prefix",
            "dst_bucket.$": "$.FinalFilesList.dst_bucket",
            "dst_prefix.$": "$.FinalFilesList.dst_prefix"
          }
        },
        ResultPath = "$.FinalProcessingResult",
        Next = "Succeed"
      },

      "Succeed" = {
        Type = "Succeed",
        OutputPath = "$"
      }
    }
  })
}

# EventBridge Rule to manually trigger the backfill job (disabled by default)
resource "aws_cloudwatch_event_rule" "backfill_trigger" {
  name                = "ncsoccer-backfill-trigger"
  description         = "Manual trigger for backfill job (disabled by default)"
  schedule_expression = "cron(0 1 1 1 ? 2099)" # Far future date, effectively disabled
  state               = "DISABLED"
}

# EventBridge Target for the backfill job
resource "aws_cloudwatch_event_target" "backfill_target" {
  rule      = aws_cloudwatch_event_rule.backfill_trigger.name
  target_id = "BackfillStepFunction"
  arn       = aws_sfn_state_machine.backfill_state_machine.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn
}

# Monitoring for the backfill job
resource "aws_cloudwatch_metric_alarm" "backfill_failure_alarm" {
  alarm_name          = "ncsoccer-backfill-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "This metric monitors backfill step function failures"
  
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.backfill_state_machine.arn
  }
  
  alarm_actions = [aws_sns_topic.ncsoccer_alarms.arn]
  ok_actions    = [aws_sns_topic.ncsoccer_alarms.arn]
}

# Log group specifically for backfill executions
resource "aws_cloudwatch_log_group" "backfill_logs" {
  name              = "/aws/states/ncsoccer-backfill"
  retention_in_days = 30
  
  tags = {
    Application = "ncsoccer"
    Component   = "backfill"
  }
}

# Lambda function for efficient backfill processing
resource "aws_lambda_function" "ncsoccer_backfill" {
  function_name    = "ncsoccer_backfill"
  image_uri        = "${aws_ecr_repository.ncsoccer.repository_url}:latest"
  package_type     = "Image"
  role             = aws_iam_role.lambda_role.arn
  memory_size      = 1024
  timeout          = 900  # 15 minutes for long-running backfill process
  
  environment {
    variables = {
      DATA_BUCKET    = aws_s3_bucket.app_data.bucket
      DYNAMODB_TABLE = "ncsh-scraped-dates"
      BACKFILL_MODE  = "true"
    }
  }
  
  ephemeral_storage {
    size = 10240  # 10GB of ephemeral storage
  }
  
  tags = {
    Name        = "ncsoccer_backfill"
    Application = "ncsoccer"
    Component   = "backfill"
  }
}

# Log group for backfill Lambda
resource "aws_cloudwatch_log_group" "backfill_lambda_logs" {
  name              = "/aws/lambda/ncsoccer_backfill"
  retention_in_days = 30
  
  tags = {
    Application = "ncsoccer"
    Component   = "backfill"
  }
}