FROM public.ecr.aws/lambda/python:3.11

# Install build dependencies
RUN yum install -y gcc gcc-c++ && \
    pip install uv

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN uv pip install --system -r requirements.txt

# Copy function code
COPY . .

# Set the CMD to your handler
CMD [ "lambda_function.handler" ]