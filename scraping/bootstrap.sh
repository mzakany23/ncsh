#!/bin/sh
# Simple bootstrap for AWS Lambda
echo "Starting AWS Lambda bootstrap..."

if [ "$BACKFILL_MODE" = "true" ]; then
  echo "Running in backfill mode"
  exec /var/lang/bin/python3 -m awslambdaric backfill_runner.lambda_handler
else
  echo "Running in standard mode"
  exec /var/lang/bin/python3 -m awslambdaric lambda_function.lambda_handler
fi
