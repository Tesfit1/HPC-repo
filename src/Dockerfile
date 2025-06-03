FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app/src

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy all source code and entrypoint script
COPY . /app/src

# Make sure entrypoint.sh is executable
RUN chmod +x /app/src/entrypoint.sh

EXPOSE 5000

# Use the entrypoint script to run AuthT.py and then start Flask
CMD ["./entrypoint.sh"]