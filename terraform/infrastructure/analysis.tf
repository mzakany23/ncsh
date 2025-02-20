locals {
  analysis_function_name = "ncsoccer-analysis"
  analysis_image_tag    = "latest"
}

# ECR Repository for Analysis Lambda
resource "aws_ecr_repository" "analysis" {
  name = var.analysis_repository
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# IAM Role for Analysis Lambda
resource "aws_iam_role" "analysis_lambda" {
  name = "ncsoccer-analysis-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Create Secrets Manager secret
resource "aws_secretsmanager_secret" "ncsoccer" {
  name        = "ncsoccer/config"
  description = "Configuration values for NC Soccer application"
}

resource "aws_secretsmanager_secret_version" "ncsoccer" {
  secret_id = aws_secretsmanager_secret.ncsoccer.id
  secret_string = jsonencode({
    openai_api_key = var.openai_api_key
    alert_email   = var.alert_email
    environment   = var.environment
  })
}

# IAM Policy for Analysis Lambda
resource "aws_iam_role_policy" "analysis_lambda" {
  name = "ncsoccer-analysis-lambda-policy"
  role = aws_iam_role.analysis_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.app_data.arn}",
          "${aws_s3_bucket.app_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "execute-api:ManageConnections"
        ]
        Resource = [
          "${aws_apigatewayv2_api.websocket.execution_arn}/*/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.analysis.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.ncsoccer.arn
        ]
      }
    ]
  })
}

# Analysis Lambda Function
resource "aws_lambda_function" "analysis" {
  function_name = local.analysis_function_name
  role          = aws_iam_role.analysis_lambda.arn
  timeout       = 300
  memory_size   = 1024
  package_type  = "Image"

  image_uri = "${aws_ecr_repository.analysis.repository_url}:${local.analysis_image_tag}"

  environment {
    variables = {
      CONFIG_SECRET   = aws_secretsmanager_secret.ncsoccer.name
      PARQUET_BUCKET = aws_s3_bucket.app_data.id
      PARQUET_KEY    = "data/parquet/data.parquet"
    }
  }

  depends_on = [aws_ecr_repository.analysis]

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# After the Lambda is created, update its environment with the Step Function ARN
resource "aws_lambda_function_event_invoke_config" "analysis" {
  function_name = aws_lambda_function.analysis.function_name

  destination_config {
    on_success {
      destination = aws_sfn_state_machine.analysis.arn
    }
  }
}

# WebSocket API Gateway
resource "aws_apigatewayv2_api" "websocket" {
  name                       = "ncsoccer-analysis-websocket"
  protocol_type             = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
}

resource "aws_apigatewayv2_stage" "websocket" {
  api_id = aws_apigatewayv2_api.websocket.id
  name   = "dev"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "websocket_lambda" {
  api_id           = aws_apigatewayv2_api.websocket.id
  integration_type = "AWS_PROXY"

  connection_type           = "INTERNET"
  content_handling_strategy = "CONVERT_TO_TEXT"
  integration_method        = "POST"
  integration_uri          = aws_lambda_function.analysis.invoke_arn
  passthrough_behavior     = "WHEN_NO_MATCH"
}

resource "aws_apigatewayv2_route" "websocket_default" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.websocket_lambda.id}"
}

resource "aws_apigatewayv2_route" "websocket_connect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$connect"
  target    = "integrations/${aws_apigatewayv2_integration.websocket_lambda.id}"
}

resource "aws_apigatewayv2_route" "websocket_disconnect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$disconnect"
  target    = "integrations/${aws_apigatewayv2_integration.websocket_lambda.id}"
}

# Lambda permission for WebSocket API
resource "aws_lambda_permission" "websocket" {
  statement_id  = "AllowWebSocketAPIInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analysis.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.websocket.execution_arn}/*/*"
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "analysis" {
  name     = "ncsoccer-analysis"
  role_arn = aws_iam_role.step_function.arn

  definition = jsonencode({
    Comment = "Soccer Analysis Pipeline"
    StartAt = "ProcessQuery"
    States = {
      ProcessQuery = {
        Type = "Task"
        Resource = aws_lambda_function.analysis.arn
        Parameters = {
          "step": "query"
          "prompt.$": "$.prompt"
          "compute_iterations.$": "$.compute_iterations"
        }
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 2
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Next = "FormatResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next = "HandleError"
        }]
      }
      FormatResult = {
        Type = "Task"
        Resource = aws_lambda_function.analysis.arn
        Parameters = {
          "step": "format"
          "prompt.$": "$.prompt"
          "result.$": "$.result"
          "format_type.$": "$.format_type"
        }
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 2
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
        Next = "SendResponse"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next = "HandleError"
        }]
      }
      SendResponse = {
        Type = "Choice"
        Choices = [
          {
            Variable = "$.mode"
            StringEquals = "stream"
            Next = "SendWebSocketResponse"
          }
        ]
        Default = "ReturnSyncResponse"
      }
      SendWebSocketResponse = {
        Type = "Task"
        Resource = aws_lambda_function.analysis.arn
        Parameters = {
          "step": "send_response"
          "connection_id.$": "$.connection_id"
          "endpoint_url.$": "$.endpoint_url"
          "result.$": "$.result"
        }
        End = true
      }
      ReturnSyncResponse = {
        Type = "Pass"
        End = true
        Parameters = {
          "statusCode": 200
          "body.$": "States.JsonToString($.result)"
        }
      }
      HandleError = {
        Type = "Pass"
        End = true
        Parameters = {
          "statusCode": 500
          "body": {
            "error.$": "$.Error"
            "cause.$": "$.Cause"
          }
        }
      }
    }
  })
}

# IAM Role for Step Functions
resource "aws_iam_role" "step_function" {
  name = "ncsoccer-analysis-step-function-role"

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

# IAM Policy for Step Functions
resource "aws_iam_role_policy" "step_function" {
  name = "ncsoccer-analysis-step-function-policy"
  role = aws_iam_role.step_function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.analysis.arn
        ]
      }
    ]
  })
}

# Add analysis outputs
output "analysis_function_name" {
  value = aws_lambda_function.analysis.function_name
}

output "websocket_api_endpoint" {
  value = aws_apigatewayv2_stage.websocket.invoke_url
}

output "analysis_step_function_arn" {
  value = aws_sfn_state_machine.analysis.arn
}