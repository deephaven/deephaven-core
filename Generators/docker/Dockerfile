FROM docker.io/library/python:3.7.10

RUN pip3 install beautifulsoup4 lxml

COPY . .

RUN set -eux; \
  python3 /python/javadocExtraction.py / false; \
  python3 /python/manualDocstrings.py / false