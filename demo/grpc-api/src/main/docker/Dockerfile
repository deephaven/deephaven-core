FROM deephaven/grpc-api:local-build

# Add the extra libraries to the classpath
COPY libs/ /app/libs

# Add the application directory
COPY app.d/ /app.d


from python:3.8
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt