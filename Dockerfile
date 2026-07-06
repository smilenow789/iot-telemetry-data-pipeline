# Use Python image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy the dependencies and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run when the container starts
CMD ["python", "main.py"]