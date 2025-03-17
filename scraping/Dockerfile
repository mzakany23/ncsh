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

# Create the bootstrap script in the correct location
# AWS Lambda will look for this file when executing the container
RUN echo '#!/bin/bash' > ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '# This is the correct bootstrap pattern that AWS Lambda expects' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '# Get the handler name from the first argument' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'handler=$1' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'if [ -z "$handler" ]; then' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  echo "ERROR: Lambda handler name is required as the first argument"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  exit 1' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'fi' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'if [ "$BACKFILL_MODE" = "true" ]; then' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  echo "Running in backfill mode with handler: $handler"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  # For Lambda we need to execute the specific handler we want' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  # Ignoring the handler argument and using our fixed handler' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  exec /var/lang/bin/python3 -m awslambdaric backfill_runner.lambda_handler' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'else' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  echo "Running in standard mode with handler: $handler"' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  # For Lambda we need to execute the specific handler we want' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  # Ignoring the handler argument and using our fixed handler' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo '  exec /var/lang/bin/python3 -m awslambdaric lambda_function.lambda_handler' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    echo 'fi' >> ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    chmod 755 ${LAMBDA_RUNTIME_DIR}/bootstrap && \
    # Create a symbolic link from /lambda-entrypoint.sh to our bootstrap script
    ln -sf ${LAMBDA_RUNTIME_DIR}/bootstrap /lambda-entrypoint.sh && \
    chmod 755 /lambda-entrypoint.sh

# Standard Lambda container configuration - no custom ENTRYPOINT required
# AWS Lambda will automatically use the bootstrap file in the correct location