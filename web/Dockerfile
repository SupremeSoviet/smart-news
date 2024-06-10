FROM python:3.9-slim

WORKDIR /app

COPY web/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY web /app/web
COPY parser /app/parser

EXPOSE 5000

ENV FLASK_APP=web/app.py
ENV FLASK_RUN_HOST=0.0.0.0

CMD ["flask", "run"]
