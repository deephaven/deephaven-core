FROM ghcr.io/deephaven/server:${VERSION:-latest}

# Replace with the desired version of jprofiler below.
RUN apt update && apt install -y wget
RUN wget https://download-gcdn.ej-technologies.com/jprofiler/jprofiler_linux_13_0.tar.gz -P /tmp/ &&\
 tar -xzf /tmp/jprofiler_linux_13_0.tar.gz -C /opt &&\
 rm -f /tmp/jprofiler_linux_8_1_2.tar.gz