FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies for pandas and pyarrow
RUN yum install -y gcc python3-devel && \
    yum clean all

# Copy requirements first for better caching
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy function code and models
COPY lambda_function.py models.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD [ "lambda_function.lambda_handler" ]