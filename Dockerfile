FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Create app directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/src /app/data /app/notebooks /app/config

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Switch back to spark user
USER 1001
