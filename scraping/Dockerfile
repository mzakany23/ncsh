# Base image for NC Soccer Scraper Lambda function
# Explicitly targeting x86_64/amd64 architecture for AWS Lambda compatibility
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11-x86_64

# Install system dependencies
RUN yum install -y gcc python3-devel && \
    yum clean all

# Copy requirements first to leverage Docker cache
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy package files and setup
COPY . ${LAMBDA_TASK_ROOT}

# Install the package
RUN cd ${LAMBDA_TASK_ROOT} && pip install -e .

# Copy lambda handler files explicitly
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
COPY backfill_runner.py ${LAMBDA_TASK_ROOT}/

# Use explicit Lambda handler format with double quotes
CMD ["lambda_function.lambda_handler"]
