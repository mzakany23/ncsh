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

# Set the handler based on BACKFILL_MODE env variable
RUN echo '#!/bin/bash\n\
if [ "$BACKFILL_MODE" = "true" ]; then\n\
  echo "Running in backfill mode"\n\
  exec python3 -m awslambdaric backfill_runner.lambda_handler\n\
else\n\
  echo "Running in standard mode"\n\
  exec python3 -m awslambdaric lambda_function.lambda_handler\n\
fi' > ${LAMBDA_TASK_ROOT}/handler.sh && \
chmod +x ${LAMBDA_TASK_ROOT}/handler.sh

# Use the script as the entry point
ENTRYPOINT [ "/var/task/handler.sh" ]