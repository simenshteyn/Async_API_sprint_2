FROM python:3.9.7-slim-buster

RUN apt-get update && apt-get install -y build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /tests && mkdir /tests/functional

WORKDIR /tests/functional

ADD requirements.txt .
RUN python3.9 -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt
ADD . .

ENV PYTHONPATH "${PYTHONPATH}:/tests"
