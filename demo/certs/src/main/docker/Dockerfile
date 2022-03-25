FROM certbot/dns-google:v1.22.0

# install curl and bash, update pip's google api dependencies
RUN apk --no-cache add curl bash && \
    python -m pip install --upgrade pip && \
    pip install --upgrade google-api-python-client>=2.33.0 google-api-core>=2.3.2

# No longer using gcloud in our script, so lets save 100s of MB. Leaving it here in case we need it again.
## Install gcloud so we can auth with our service account
#RUN curl -sSL https://sdk.cloud.google.com | bash
#ENV PATH $PATH:/root/google-cloud-sdk/bin
#ENV CLOUDSDK_CONFIG=/root/.config
#RUN echo " \
#CLOUDSDK_CONFIG=/root/.config/gcloud \
#" > /etc/environment ;

# Set variable used by docker-entrypoint.sh
ENV SECRET=deephaven-app-cert
ENV EMAIL=operations@deephaven.io
ENV DOMAIN=demo.deephaven.app
ENV DEBUG=false

# Copy our scripts and make them executable
COPY ./rx /rx
RUN chmod -R +x /rx

ENTRYPOINT ["/rx/docker-entrypoint.sh"]