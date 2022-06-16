# Select base-image
FROM python:3.10.0-slim-bullseye

# Load python requirements-file
COPY app/requirements.txt /

# Upgrade pip and install requirements
RUN apt-get update && apt install -y git 
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt --no-cache-dir && \
    rm requirements.txt

# Copy required files into container
COPY app/* tests/valid-testdata/* /app/
RUN mkdir /app/weatherforecasts/

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Run the application
CMD ["python3", "-u", "/app/main.py"]
