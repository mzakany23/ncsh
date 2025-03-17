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

# Use a much simpler bootstrap approach
# This should be more reliable across different environments
COPY bootstrap.sh /var/runtime/bootstrap
RUN chmod 755 /var/runtime/bootstrap && \
    ln -sf /var/runtime/bootstrap /lambda-entrypoint.sh && \
    chmod 755 /lambda-entrypoint.sh && \
    # Verify permissions and content 
    ls -la /var/runtime/bootstrap && \
    ls -la /lambda-entrypoint.sh && \
    cat /var/runtime/bootstrap

# Standard Lambda container configuration - no custom ENTRYPOINT required
# AWS Lambda will automatically use the bootstrap file in the correct location