FROM python:3-stretch
MAINTAINER Uri Savelchev <alterrebe@gmail.com>

RUN apt-get update -qq && \
    apt-get install -qqy postgresql-client && \
    pip3 install psycopg2

COPY run.py /run.py

ENTRYPOINT [ "/run.py" ]

# Don't forget export PGSSLMODE=require to connect using psql
