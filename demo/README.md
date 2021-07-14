This directory contains the helm chart(s) used for the dhdemo application.

For now, this will be a localhost minikube deployment only.  
Once we are happy with this setup, we'll move on to a production setup.

If you are a Deephaven developer / contributor,  
make sure you have setup minikube on your local machine  
Ubuntu users: https://phoenixnap.com/kb/install-minikube-on-ubuntu



Notes:

Getting locally built images into minikube:

./gradlew prepareCompose
docker-compose build
docker-compose up

Then, tag and import images:
docker tag deephaven/grpc-api:latest deephaven/grpc-api:local
docker tag deephaven/grpc-proxy:latest deephaven/grpc-proxy:local
docker tag deephaven/web:latest deephaven/web:local
docker tag deephaven/envoy:latest deephaven/envoy:local

minikube image load deephaven/grpc-api:local
minikube image load deephaven/grpc-proxy:local
minikube image load deephaven/web:local
minikube image load deephaven/envoy:local


There! Now you can reference grpc-api:local images in kubernets/minikube