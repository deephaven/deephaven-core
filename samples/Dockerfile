FROM alpine:3.7
RUN apk add --no-cache git git-lfs

VOLUME [ "/data" ]
ENTRYPOINT [ "/sample_script.sh" ]

COPY sample_script.sh .

