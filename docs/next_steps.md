# Next Steps and Testing Instructions

## Implementation Summary

We've implemented a complete unified date range workflow with batching to address the Lambda timeout issues during monthly operations. The implementation includes:

1. **New Components:**
   - Utility Lambda functions for input validation, batch planning, and result verification
   - Step Function definition with Map state for parallel batch processing
   - Trigger script for easy workflow invocation
   - Documentation and implementation plan

2. **Modifications:**
   - Updated scraper Lambda function to handle batched dates
   - Updated deployment workflow to include the new utility functions
   - Updated README and CHANGELOG with details of the changes

## Testing Instructions

### Prerequisites

1. Ensure you have AWS CLI configured with the appropriate profile:
   ```bash
   aws configure list --profile your-profile-name
   ```

2. Ensure you have Terraform installed:
   ```bash
   terraform --version
   ```

### Local Testing

1. **Test the trigger script locally:**
   ```bash
   python scripts/trigger_batched_workflow.py --date 2024-03-01 --profile your-profile-name
   ```

2. **Test with a small date range:**
   ```bash
   python scripts/trigger_batched_workflow.py --date-range 2024-03-01 2024-03-03 --profile your-profile-name
   ```

3. **Test with a full month:**
   ```bash
   python scripts/trigger_batched_workflow.py --month 2024 3 --profile your-profile-name
   ```

### Infrastructure Deployment

1. **Deploy the infrastructure:**
   ```bash
   cd terraform/infrastructure
   terraform init
   terraform plan
   terraform apply
   ```

2. **Verify Lambda functions:**
   - Check that the new Lambda functions were created:
     - `ncsoccer-input-validator`
     - `ncsoccer-batch-planner`
     - `ncsoccer-batch-verifier`

3. **Verify Step Function:**
   - Check that the new Step Function was created:
     - `ncsoccer-unified-workflow-batched`

### Production Testing

1. **Test with a single day:**
   ```bash
   python scripts/trigger_batched_workflow.py --date 2024-03-01 --profile prod
   ```

2. **Test with a small date range:**
   ```bash
   python scripts/trigger_batched_workflow.py --date-range 2024-03-01 2024-03-03 --profile prod
   ```

3. **Test with a full month:**
   ```bash
   python scripts/trigger_batched_workflow.py --month 2024 3 --profile prod
   ```

4. **Verify results in S3:**
   ```bash
   aws s3 ls s3://ncsh-app-data/data/games/year=2024/month=03/ --recursive --profile prod
   ```

## Troubleshooting

### Common Issues

1. **Lambda Timeouts:**
   - Check CloudWatch Logs for Lambda functions
   - Adjust batch size if needed

2. **Permission Issues:**
   - Verify IAM roles and policies
   - Check that Lambda functions have appropriate permissions

3. **Data Quality Issues:**
   - Verify data in S3 for completeness
   - Check for any missing days or batches

### Useful Commands

1. **Check Lambda logs:**
   ```bash
   aws logs get-log-events --log-group-name /aws/lambda/ncsoccer-scraper --profile your-profile-name
   ```

2. **Describe Step Function execution:**
   ```bash
   aws stepfunctions describe-execution --execution-arn <execution-arn> --profile your-profile-name
   ```

3. **List S3 objects:**
   ```bash
   aws s3 ls s3://ncsh-app-data/data/games/ --recursive --profile your-profile-name
   ```

## Next Steps

1. **Performance Analysis:**
   - Monitor the performance of the new workflow
   - Compare execution times with the previous implementation
   - Adjust batch size if needed for optimal performance

2. **User Feedback:**
   - Collect feedback from users on the new workflow
   - Address any issues or concerns

3. **Further Enhancements:**
   - Consider adding CloudWatch Alarms for workflow failures
   - Implement a dashboard for monitoring workflow executions
   - Add more detailed metrics for performance analysis

## Conclusion

The unified date range workflow with batching addresses the Lambda timeout issues during monthly operations. It provides a more reliable, efficient, and scalable approach to scraping data. The implementation is complete and ready for testing and deployment.

Remember to monitor performance and collect feedback to ensure the solution meets all requirements.