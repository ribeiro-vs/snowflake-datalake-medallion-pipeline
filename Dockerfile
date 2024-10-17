# Using an official Python runtime as a parent image
FROM python:3.9-slim

# Setting the working directory in the container
WORKDIR /usr/src/app

# Copying the requirements file into the container
COPY requirements.txt ./

# Installing any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copying the rest of the application code
COPY . .

# Running the application
CMD ["python", "./app.py"]