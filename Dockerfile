FROM python:3.11

RUN mkdir /app
WORKDIR /app

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
    ffmpeg \
    && apt-get clean

COPY requirements.txt .
RUN pip install -r requirements.txt gunicorn

COPY *.py ./

EXPOSE 80

CMD exec gunicorn --bind 0.0.0.0:80 main:APP --threads 1 --workers 1 --access-logfile -
