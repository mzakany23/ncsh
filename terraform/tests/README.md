# Terraform Test Files

This directory contains test files and examples for the NC Soccer Terraform infrastructure.

## Files

- `test-versioning-input.json` - Example input for testing the unified workflow with dataset versioning

## Usage

These test files can be used with the AWS CLI to manually invoke step functions:

```bash
# Example: Test the unified workflow with versioning parameters
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-2:552336166511:stateMachine:ncsoccer-unified-workflow \
  --input file://terraform/tests/test-versioning-input.json \
  --region us-east-2
```

This allows for testing specific operations without modifying the production setup.

## Notes

- The unified workflow step function should be used for all operations, as it consolidates functionality from previous separate workflows
- No Terraform changes need to be applied to use these test files