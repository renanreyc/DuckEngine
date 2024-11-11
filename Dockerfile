FROM python:3.10-slim

WORKDIR /app

COPY src/main/libs/httpfs /app/main/libs/httpfs
RUN pip install /app/main/libs/httpfs


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src/main .
# COPY ./.env ./.env

ENTRYPOINT ["python", "main.py"]