# The base image
# Load python container and install required python libs
FROM python:3.10.0-slim-bullseye
RUN pip3 install kafka-python pandas

# Cpy required files into container
COPY forecast-import.py gridpoints.csv /

# Run the application
CMD ["python3", "-u", "forecast-import.py"]