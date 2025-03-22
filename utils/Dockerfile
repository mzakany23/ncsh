FROM public.ecr.aws/lambda/python:3.11

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy utility Lambda functions
COPY src/ .

# Command can be overridden by providing a different command in the template directly.
CMD ["lambda_function.handler"]