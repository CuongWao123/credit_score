FROM apache/spark:3.5.7

USER root

# Install Python dependencies
RUN pip install --no-cache-dir requests

# Create ivy cache directory
RUN mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && \
    chmod -R 777 /tmp/.ivy2

# Set environment
ENV IVY_HOME=/tmp/.ivy2

USER 185

WORKDIR /opt/spark
