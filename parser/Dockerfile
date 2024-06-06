# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install ClickHouse certificates
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex && \
    wget "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" \
    --output-document /usr/local/share/ca-certificates/Yandex/RootCA.crt && \
    wget "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem" \
    --output-document /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt && \
    chmod 655 \
    /usr/local/share/ca-certificates/Yandex/RootCA.crt \
    /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt && \
    update-ca-certificates

# Copy the rest of the working directory contents into the container at /app
COPY . .

# Expose port 80
EXPOSE 80

# Run parser.py when the container launches
CMD ["python", "parser.py"]
