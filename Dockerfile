FROM python:3.6

ADD . /app

RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y libpq-dev python3-dev && \
  pip3 install -r /app/requirements.txt && \
  pip3 install -e /app

ENTRYPOINT tail -f /dev/null
