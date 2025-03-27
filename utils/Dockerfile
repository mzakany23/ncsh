# Base image for NC Soccer Utils Lambda functions
# Explicitly targeting x86_64/amd64 architecture for AWS Lambda compatibility
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11-x86_64

# Copy requirements first to leverage Docker cache
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy all source files to the Lambda task root
COPY src/ ${LAMBDA_TASK_ROOT}/

# Use explicit Lambda handler format
CMD ["lambda_function.handler"]