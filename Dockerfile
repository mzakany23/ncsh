# Base image for NC Soccer Scraper Lambda function
# Handles both day and month mode scraping operations
# Using SHA-tagged images for versioning
FROM public.ecr.aws/lambda/python:3.11

# Create and activate virtual environment
RUN python -m venv ${LAMBDA_TASK_ROOT}/.venv
ENV PATH="${LAMBDA_TASK_ROOT}/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="${LAMBDA_TASK_ROOT}/.venv"

# Add build argument to force fresh pip installs
ARG BUILD_DATE=unknown

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages in the virtual environment
RUN pip install -r requirements.txt

# Copy function code
COPY ncsoccer/ ${LAMBDA_TASK_ROOT}/ncsoccer/
COPY runner.py ${LAMBDA_TASK_ROOT}

# Copy lambda handler
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "lambda_function.handler" ]