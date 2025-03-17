#!/bin/bash
# Script to properly build Lambda container images for AWS Lambda x86_64 environment
# This is especially important when developing on ARM-based Macs (M1/M2/M3)

set -e

# Default tag is latest
TAG=${1:-latest}
REPO_NAME="ncsoccer-scraper"

# Check architecture
ARCH=$(uname -m)
echo "Building on architecture: $ARCH"

if [[ "$ARCH" == "arm64" ]]; then
  echo "Detected ARM64 architecture - will explicitly build for x86_64 target"
  # First clear any cached images that might have the wrong architecture
  echo "Cleaning up any previous images..."
  docker rmi $REPO_NAME:$TAG 2>/dev/null || true
  
  # Build with explicit platform flag for AMD64 (x86_64)
  echo "Building Docker image for x86_64 architecture..."
  cd scraping && docker build --platform=linux/amd64 -t $REPO_NAME:$TAG -f Dockerfile .
else
  echo "Building Docker image on x86_64 architecture..."
  cd scraping && docker build -t $REPO_NAME:$TAG -f Dockerfile .
fi

# Show the image details to verify architecture
echo "Verifying image architecture..."
docker inspect $REPO_NAME:$TAG | grep -A 3 Architecture || echo "Could not determine architecture"

echo "Image built successfully with tag: $REPO_NAME:$TAG"
echo ""
echo "To push to ECR:"
echo "1. Login to ECR: aws ecr get-login-password | docker login --username AWS --password-stdin <your-account-id>.dkr.ecr.<region>.amazonaws.com"
echo "2. Tag the image: docker tag $REPO_NAME:$TAG <your-account-id>.dkr.ecr.<region>.amazonaws.com/$REPO_NAME:$TAG"
echo "3. Push the image: docker push <your-account-id>.dkr.ecr.<region>.amazonaws.com/$REPO_NAME:$TAG"
