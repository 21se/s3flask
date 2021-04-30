FROM python:alpine
RUN mkdir /app
WORKDIR /app
COPY . .
RUN pip install -r /app/requirements.txt
RUN apk add redis
EXPOSE 5000
RUN chmod +x /app/entrypoint.sh

ENV REDIS_SERVER=localhost
ENV REDIS_PORT=6379
ENV MINIO_SERVER=localhost
ENV MINIO_PORT=9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV FLASK_SERVER=0.0.0.0
ENV FLASK_PORT=5000

ENTRYPOINT ["sh","/app/entrypoint.sh"]