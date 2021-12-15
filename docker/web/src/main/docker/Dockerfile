FROM deephaven/nginx-base:local-build

COPY licenses/ /
COPY nginx/default.conf /etc/nginx/conf.d/
COPY nginx/nginx.conf /etc/nginx/
COPY nginx/99-init-notebooks.sh /docker-entrypoint.d

COPY static/ /usr/share/nginx/html

VOLUME /tmp
