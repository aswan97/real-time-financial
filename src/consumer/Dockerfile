FROM python:3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir -p /app/data
COPY . .
CMD ["python", "consumer.py"]