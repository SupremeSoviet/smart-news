# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install necessary tools and packages specified in requirements.txt
RUN apt-get update && \
    apt-get install -y wget ca-certificates && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the working directory contents into the container at /app
COPY . .

# Expose port 80
EXPOSE 80

# Run parser.py when the container launches
CMD ["python", "parser.py"]
