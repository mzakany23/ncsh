FROM public.ecr.aws/lambda/python:3.11

# Set up the app directory
COPY requirements.txt pyproject.toml ./
COPY models.py lambda_function.py ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set up the entrypoint
CMD [ "lambda_function.lambda_handler" ]