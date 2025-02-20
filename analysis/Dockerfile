FROM public.ecr.aws/lambda/python:3.11

# Install build dependencies
RUN yum install -y gcc gcc-c++ && \
    pip install uv

# Copy requirements files
COPY requirements.in .

# Install Python dependencies
RUN uv pip compile requirements.in -o requirements.txt && \
    uv pip install -r requirements.txt

# Copy function code
COPY . .

# Set the CMD to your handler
CMD [ "lambda_function.handler" ]