command -v kubectl || {
    log "kubectl not already installed, installing from snap"
    sudo snap install kubectl --classic
}
CLUSTER_NAME="${CLUSTER_NAME:-dhce-auto}"
PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
ZONE="${ZONE:-us-central1}"
K8S_CONTEXT="gke_${PROJECT_ID}_${ZONE}_${CLUSTER_NAME}"
DH_SSL_DIR="${DH_SSL_DIR:-/etc/ssl/dh}"

export HOME="${HOME:-/root}"
mkdir -p "$HOME/.kube"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"


gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --verbosity debug

kubectl config use-context "${K8S_CONTEXT}"

# Cannot be run from node, but you may need to change the service account if you didn't create your machine correctly
#gcloud compute instances set-service-account YOUR_MACHINE_NAME_HERE \
#    --service-account dh-controller@deephaven-oss.iam.gserviceaccount.com
#    --scopes https://www.googleapis.com/auth/cloud-platform


# Pull certificates out of kubectl
test -s "$DH_SSL_DIR/tls.key" ||
    { kubectl get secret deephaven-app-cert -o go-template='{{index .data "tls.key" | base64decode}}' | sudo tee "$DH_SSL_DIR/tls.key" > /dev/null ; }
test -s "$DH_SSL_DIR/tls.key.pk8" ||
    { sudo openssl pkcs8 -topk8 -nocrypt -in "$DH_SSL_DIR/tls.key" -out "$DH_SSL_DIR/tls.key.pk8" ; sudo chmod a+r "$DH_SSL_DIR/tls.key.pk8" ; }
# TODO: also use openssl checkend on tls.crt...
{ test -s "$DH_SSL_DIR/tls.crt" ; } ||
    { kubectl get secret deephaven-app-cert -o go-template='{{index .data "tls.crt" | base64decode}}' | sudo tee "$DH_SSL_DIR/tls.crt" > /dev/null ; }
{ test -s "$DH_SSL_DIR/ca.crt" ; } ||
    { kubectl get secret deephaven-app-ca -o go-template='{{index .data "ca.crt" | base64decode}}' | sudo tee "$DH_SSL_DIR/ca.crt" > /dev/null ; }

# hm... we should probably delete our kubernetes credentials now that we don't need them anymore...
# though, tbh, when debugging broken setup, having credentials in place is nice (you should be able to copy-paste this script)