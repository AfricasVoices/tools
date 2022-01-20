FROM python:3.8-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Make a directory for the output data
RUN mkdir /data

# Make a directory for private credentials files
RUN mkdir /credentials

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD . /app
