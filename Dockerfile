FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Set environment variable for Python to write output unbuffered
ENV PYTHONUNBUFFERED=1
ENV HYDRO_DATA_DIR=/app/data

# Run the application
CMD ["python", "main.py"]