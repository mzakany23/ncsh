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

# Copy package files
COPY ncsoccer ${LAMBDA_TASK_ROOT}/ncsoccer

# Copy lambda handler
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (matches the function name in lambda_function.py)
CMD [ "lambda_function.lambda_handler" ]