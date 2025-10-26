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

USER airflow

# Set the PATH for the airflow user
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python dependencies (MariaDB driver)
RUN pip install --no-cache-dir mariadb

# Install your custom provider
# Make sure you install it without upgrading core Airflow packages
COPY airflow-mariadb-provider /opt/airflow/airflow-mariadb-provider
RUN pip install --no-deps /opt/airflow/airflow-mariadb-provider