# Start with an official Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Install PostgreSQL client tools for pg_isready
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the etl directory's contents into the container
COPY . .

# Make the wait script executable
RUN chmod +x wait-for-postgres.sh
