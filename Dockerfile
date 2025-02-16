# Base image for NC Soccer Scraper Lambda function
# Handles both day and month mode scraping operations
# Using SHA-tagged images for versioning
FROM public.ecr.aws/lambda/python:3.11

# Removed virtualenv creation; install dependencies globally

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages globally
RUN pip install -r requirements.txt

# Copy function code
COPY ncsoccer/ ${LAMBDA_TASK_ROOT}/ncsoccer/
COPY runner.py ${LAMBDA_TASK_ROOT}
COPY scrapy.cfg ${LAMBDA_TASK_ROOT}

# Copy lambda handler
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "lambda_function.handler" ]