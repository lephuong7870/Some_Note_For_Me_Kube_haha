FROM python:3.9-slim
RUN apt-get update 

WORKDIR /app

COPY app/. /app/.
COPY models/. /models/.
COPY requirements.txt /app/
COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

RUN pip install --no-cache-dir -r /app/requirements.txt 
EXPOSE 8000
ENV PYTHONPATH=/app

ENTRYPOINT ["/app/entrypoint.sh"]

