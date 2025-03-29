# Use the official Airflow image as the base
FROM apache/airflow:2.10.2

# Switch to root to install system dependencies
USER root


# Install any additional system packages (e.g., PostgreSQL client, Redis client, etc.)
# Install Chrome for Selenium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    chromium \
    chromium-driver \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libappindicator1 \
    fonts-liberation \
    xdg-utils \
    libu2f-udev \
    libgtk-3-0 \
    libasound2 \
    libdrm2 \
    libgbm1 \
    --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get install -y chromium-driver



# Set environment variables for GitPython
ENV GIT_PYTHON_REFRESH=quiet

# Switch back to airflow user
USER airflow

# Copy requirements.txt and install Python dependencies

#RUN pip install poetry

#COPY pyproject.toml poetry.lock ./

#RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


# Copy your DAGs and other files
COPY dags /opt/airflow/dags

# Set environment variables
ENV AIRFLOW_UID=50000
ENV AIRFLOW_HOME=/opt/airflow

# Expose the webserver port
EXPOSE 8080

# Copy the entrypoint script to the container
COPY entrypoint.sh /entrypoint.sh

# Make the entrypoint script executable
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Set the entrypoint script as the default command
ENTRYPOINT ["/entrypoint.sh"]