# Use the official Apache Airflow image as the base
FROM apache/airflow:2.9.2

# Set the working directory
WORKDIR /opt/airflow

# Install additional Python dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Expose necessary ports
EXPOSE 8080

# Set default command
CMD ["airflow", "webserver"]
