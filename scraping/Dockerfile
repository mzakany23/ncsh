# Base image for NC Soccer Scraper Lambda function
# Handles both day and month mode scraping operations
# Using SHA-tagged images for versioning
FROM public.ecr.aws/lambda/python:3.11

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

# Copy lambda handler and backfill runner
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY backfill_runner.py ${LAMBDA_TASK_ROOT}

# Create a wrapper script in the correct location with proper line breaks
RUN echo '#!/bin/bash' > ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '# Accept handler name from Lambda runtime as first argument' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'if [ "$BACKFILL_MODE" = "true" ]; then' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  echo "Running in backfill mode"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  exec python3 -m awslambdaric backfill_runner.lambda_handler "$@"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'else' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  echo "Running in standard mode"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  exec python3 -m awslambdaric lambda_function.lambda_handler "$@"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'fi' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    chmod 755 ${LAMBDA_RUNTIME_DIR}/bootstrap

# Standard Lambda container configuration - no custom ENTRYPOINT required
# AWS Lambda will automatically use the bootstrap file in the correct location