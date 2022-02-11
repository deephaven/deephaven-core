FROM python:3

RUN apt-get update && apt-get -qy install curl

RUN pip install barnum kafka-python mysql-connector-python requests wait-for-it

COPY . /loadgen

COPY docker-entrypoint.sh /usr/local/bin

ENTRYPOINT ["docker-entrypoint.sh"]
