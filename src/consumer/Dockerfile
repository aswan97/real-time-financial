FROM python:3.11

WORKDIR /app

# Install the requirments
COPY /src/consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy both the common directory for the utility functions and the consumer script
COPY src/common ./common    
COPY src/consumer ./consumer    

# Tells python where to look for the modules
ENV PYTHONPATH=/app

CMD ["python", "consumer/consumer.py"]