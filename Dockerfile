FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src /app/src

# Expose Spark UI port
EXPOSE 4040

# Default command (can be overridden)
CMD ["python3", "src/productor.py"]
