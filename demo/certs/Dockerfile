FROM certbot/dns-google

COPY secret-patch-template.json /
COPY docker-entrypoint.sh /
COPY google-svc.json /

RUN chmod 600 /google-svc.json

CMD ["/docker-entrypoint.sh"]