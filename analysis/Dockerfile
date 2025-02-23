# Use Python 3.11 as base image
FROM python:3.11-slim

# Install build dependencies and uv
RUN apt-get update && \
    apt-get install -y curl build-essential && \
    pip install uv

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.in .

# Compile and install dependencies using uv in system mode
RUN uv pip compile requirements.in -o requirements.txt && \
    uv pip install --system -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV OPENAI_API_KEY=""

# Create __init__.py if it doesn't exist
RUN touch __init__.py

# Expose Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "ui/Home.py", "--server.address", "0.0.0.0"]