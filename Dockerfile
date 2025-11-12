FROM python:3.11-slim-bullseye

# Install system dependencies including Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    build-essential \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

# Create app directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/src /app/data /app/notebooks /app/config

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Default command
CMD ["bash"]
