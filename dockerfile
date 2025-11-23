FROM apache/airflow:3.1.2

# Switch to root user for system package installations
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    # Required for building some Python packages like psycopg2 (Postgres driver)
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for Python package installations
USER airflow

# Install all necessary Python packages
# NOTE: The versions below are updated to recent, stable releases.
RUN pip install --no-cache-dir \
    # Airflow providers for connecting to external services
    apache-airflow-providers-postgres==5.11.3 \
    apache-airflow-providers-telegram==4.8.3 \
    \
    # Data processing and ML stack
    pandas==2.2.2 \
    scikit-learn==1.4.2 \
    \
    # Web/API and scraping tools
    requests==2.32.3 \
    beautifulsoup4==4.12.3 \
    yfinance==0.2.35 \
    # Install psycopg2-binary for PostgreSQL connection, often faster than building from source
    psycopg2-binary==2.9.9