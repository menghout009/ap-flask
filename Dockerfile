# Use an official Python runtime as a parent image
FROM python:3-alpine3.9

WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install system dependencies
RUN apk update && apk upgrade && \
    apk add --no-cache build-base libffi-dev openssl-dev libxslt-dev libxml2-dev postgresql-dev musl-dev

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install Flask flask_restx psycopg2

# Install Scrapy
RUN pip install scrapy==2.9.0
RUN pip install flask_jwt_extended
RUN pip install apscheduler passlib flask_cors chatter Flask-Cors==4.0.0 passlib==1.7.4 scrapydo==0.2.2 chatterbot==1.0.4 && \
PyYAML==3.12
RUN pip install cachetools

# Install other dependencies
RUN pip install googletrans gunicorn scrapydo

# Copy the rest of your application's source code into the container
COPY . .

# Expose the port on which your Flask app will run (replace with your app's port)
EXPOSE 8000
CMD ["flask", "run", "--host", "0.0.0.0" ,"-p", "8000"]
# Define the command to run your Flask app using Gunicorn