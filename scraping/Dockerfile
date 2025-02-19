# Base image for NC Soccer Scraper Lambda function
# Handles both day and month mode scraping operations
# Using SHA-tagged images for versioning
FROM public.ecr.aws/lambda/python:3.11

# Removed virtualenv creation; install dependencies globally

# Copy requirements.txt
COPY scraping/requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages globally
RUN pip install -r requirements.txt

# Copy function code
COPY scraping/ncsoccer/ ${LAMBDA_TASK_ROOT}/ncsoccer/
COPY scraping/runner.py ${LAMBDA_TASK_ROOT}
COPY scraping/scrapy.cfg ${LAMBDA_TASK_ROOT}

# Copy lambda handler
COPY scraping/lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (matches the function name in lambda_function.py)
CMD [ "lambda_function.lambda_handler" ]