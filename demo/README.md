This directory contains the helm chart(s) used for the dhdemo application.

For now, this will be a localhost minikube deployment only.  
Once we are happy with this setup, we'll move on to a production setup.

If you are a Deephaven developer / contributor,  
make sure you have setup minikube on your local machine  
Ubuntu users: https://phoenixnap.com/kb/install-minikube-on-ubuntu



Notes:

Getting locally built images into minikube:

Option 1 (build in minikube):
eval $(minikube docker-env)
./gradlew prepareCompose
# now all images are built inside minikube

Option 2 (import into minikube):

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

# To change envoy log levels:
`<insert instructions how to shell into container through k8 pod>`
curl -X POST localhost:9090/logging?level=trace

minikube service --url dh-local






Notes for customers using our helm chart (someday):

Step 1: install minikube OR enable Google Kubernetes Engine

1a (minikube): link to setup guide
1b (gke): create kubernetes cluster
-> use autopilot
-> select Public cluster (you can get private to work, but we're using public for simplicity)


CLUSTER_NAME=dhce-auto
PROJECT_ID=deephaven-oss
ZONE=us-central1
K8S_CONTEXT=gke_"$PROJECT_ID"_"$ZONE"_"$CLUSTER_NAME"
K8S_NAMESPACE=dh
DOCKER_VERSION=0.0.3

https://console.cloud.google.com/artifacts/create-repo?project=deephaven-oss

gcloud artifacts repositories create deephaven \
--repository-format=docker \
--location=$ZONE \
--description="Docker repository"

gcloud auth configure-docker ${ZONE}-docker.pkg.dev


docker tag deephaven/grpc-proxy:local-build ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/grpc-proxy:$DOCKER_VERSION
docker tag deephaven/grpc-api:local-build ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/grpc-api:$DOCKER_VERSION
docker tag deephaven/web:local-build ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/web:$DOCKER_VERSION

enable artifact registry:
https://console.cloud.google.com/apis/library/artifactregistry.googleapis.com?project=deephaven-oss

docker push ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/grpc-proxy:$DOCKER_VERSION
docker push ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/grpc-api:$DOCKER_VERSION
docker push ${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven/web:$DOCKER_VERSION


gcloud container clusters get-credentials "${CLUSTER_NAME}" \
--zone "${ZONE}" \
--project "${PROJECT_ID}"
kubectl config use-context "${K8S_CONTEXT}"
kubectl config get-contexts
kubectl config current-context

now, deploy the app!

kubectl apply -f demo/dh-prod.yaml

Next, setup some public DNS for your deployment
(TODO: get this moved to kubectl)

gcloud beta container clusters update $CLUSTER_NAME --project $PROJECT_ID --zone $ZONE --cluster-dns clouddns --cluster-dns-scope cluster









https://cloud.google.com/kubernetes-engine/docs/how-to/cloud-dns

gcloud beta container clusters create CLUSTER_NAME \
--cluster-dns clouddns --cluster-dns-scope cluster \
--cluster-version VERSION | --release-channel \
[--zone ZONE_NAME | --region REGION_NAME]



kubectl expose pod $pod --name dns-test --port 8080




{ kubectl delete deployment dh-local || true ; } && kubectl apply -f demo/dh-localhost.yaml



k get pods -o wide
# find node name
kubectl get nodes --output wide
# get external IP from the node you saw in pod list
gcloud compute firewall-rules create dh-api --project ${PROJECT_ID} --allow tcp:30080
gcloud compute firewall-rules create dh-admin --project ${PROJECT_ID} --allow tcp:30443


Setup DNS:

DNS_ZONE=dhce-zone
DOMAIN_ROOT=deephavencommunity.com
NODE_IP=35.224.115.186
MACHINE_NAME=dhce

gcloud beta dns --project=${PROJECT_ID} managed-zones create ${DNS_ZONE} --description="DNS for Deephaven" --dns-name="${DOMAIN_ROOT}." --visibility="public" --dnssec-state="off"

gcloud dns --project=${PROJECT_ID} record-sets transaction start --zone=${DNS_ZONE}
gcloud dns --project=${PROJECT_ID} record-sets transaction add ${NODE_IP} --name=${MACHINE_NAME}.${DOMAIN_ROOT}. --ttl=300 --type=A --zone=${DNS_ZONE}
gcloud dns --project=${PROJECT_ID} record-sets transaction execute --zone=${DNS_ZONE}

