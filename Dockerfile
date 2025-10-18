# Use the official Airflow image
FROM apache/airflow:2.9.0

# Switch to root user to install system dependencies
USER root

# Install system dependencies for MariaDB
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libmariadb-dev \
        mariadb-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set the PATH for the airflow user
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python dependencies (MariaDB driver)
RUN pip install --no-cache-dir mariadb

# Copy and install your custom provider
COPY --chown=airflow:airflow ./airflow-mariadb-provider /opt/airflow/.local/src/airflow-mariadb-provider
RUN pip install --no-cache-dir -e /opt/airflow/.local/src/airflow-mariadb-provider
