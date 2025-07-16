FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9000

CMD ["flask", "run", "--host=0.0.0.0", "--port=9000"]
