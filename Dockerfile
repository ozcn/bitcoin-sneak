FROM python:3.6

ADD . /app

RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y libpq-dev python3-dev && \
  pip install -r /app/requirements.txt && \
  pip install -e /app

