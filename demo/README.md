This directory contains the helm chart(s) used for the dhdemo application.

For now, this will be a localhost minikube deployment only.  
Once we are happy with this setup, we'll move on to a production setup.

If you are a Deephaven developer / contributor,  
make sure you have setup minikube on your local machine  
Ubuntu users: https://phoenixnap.com/kb/install-minikube-on-ubuntu



Notes:

Getting locally built images into minikube:

./gradlew prepareCompose
docker-compose push

Then, import images to minikube:

minikube image load deephaven/grpc-api:local-build
minikube image load deephaven/grpc-proxy:local-build
minikube image load deephaven/web:local-build
minikube image load deephaven/envoy:local-build

minikube image load deephaven/grpc-api:local-build  deephaven/grpc-proxy:local-build  deephaven/web:local-build deephaven/envoy:local-build


There! Now you can reference grpc-api:local images in kubernets/minikube

# All in one Update Minikube:
./gradlew preCo && docker-compose push && minikube image load deephaven/grpc-api:local-build  deephaven/grpc-proxy:local-build  deephaven/web:local-build deephaven/envoy:local-build