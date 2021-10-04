#!/usr/bin/env bash

#set -o nounset
set -o errexit
set -o xtrace

# Save some logs for us to debug any issues
LOG_FILE="/var/log/vm-startup.log-$(date "+%Y%m%d%H%M%S")"
sudo ln -nsf $LOG_FILE /var/log/vm-startup.log
exec > >(tee "$LOG_FILE" | logger -t vm-startup -s 2>/dev/console) 2>&1

# Prepare a nice logging function
SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
H_NAME="$(hostname)"
function log() {
  echo "$SCRIPT_NAME -- $H_NAME -- $(date) $*"
}

# Make sure we meet system requirements
apt_needs=""
command -v docker || apt_needs=" $apt_needs docker"
command -v docker-compose || apt_needs=" $apt_needs docker-compose"
command -v curl || apt_needs=" $apt_needs curl"
test -n "$apt_needs" && {
    log "$apt_needs not already installed, installing from apt"
    sudo apt update -y
    sudo apt install -y $apt_needs
    sudo apt clean
}
command -v kubectl || {
    log "kubectl not already installed, installing from snap"
    sudo snap install kubectl --classic
}


MY_UNAME="$(id -un)"
MY_GNAME="$(id -gn)"
req_id="${MY_UNAME}:${MY_GNAME}"

CLUSTER_NAME=dhce-auto
PROJECT_ID=deephaven-oss
ZONE=us-central1
K8S_CONTEXT=gke_"$PROJECT_ID"_"$ZONE"_"$CLUSTER_NAME"

# Prepare the deephaven working directory
DH_DIR="${DH_DIR:-/dh}"
export HOME=${HOME:-/root}
mkdir -p $HOME/.kube
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
DATA_DIR="${DATA_DIR:-$DH_DIR/data}"
DH_SSL_DIR="${DH_SSL_DIR:-/etc/ssl/dh}"
test -d "$DH_DIR" ||
    sudo mkdir -p "$DH_DIR"
test "$req_id" == "$(stat -c %U:%G "$DH_DIR")" ||
    sudo chown "$(id -un):$(id -gn)" "$DH_DIR"
test -d "$DATA_DIR" ||
    mkdir -p "$DATA_DIR"
test -d "$DH_SSL_DIR" ||
    sudo mkdir -p "$DH_SSL_DIR"

gcloud container clusters get-credentials "${CLUSTER_NAME}" \
--zone "${ZONE}" \
--project "${PROJECT_ID}" \
--verbosity debug

kubectl config use-context "${K8S_CONTEXT}"

# Cannot be run from node, but you may need to change the service account if you didn't create your machine correctly
#gcloud compute instances set-service-account YOUR_MACHINE_NAME_HERE \
#    --service-account dh-controller@deephaven-oss.iam.gserviceaccount.com
#    --scopes https://www.googleapis.com/auth/cloud-platform


# Make sure docker group exists, and our running user is in it.
# Warning: this leaks root access to logged in user account
# we should instead use rootless docker: https://docs.docker.com/engine/security/rootless
getent group docker > /dev/null || sudo groupadd docker
getent group docker | grep -c -E "[:,]$MY_UNAME" || sudo usermod -aG docker $MY_UNAME


# While we _should_ get docker-compose.yml from github via curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml
# the version there isn't parameterized like this one, as we want to replace the docker repo with our demo-specific one
test -f "$DH_DIR/docker-compose.yml" || cat << 'EOF' > "$DH_DIR/docker-compose.yml"
version: "3.4"

services:
  demo-server:
    image: ${REPO:-ghcr.io/deephaven}/demo-server:${VERSION:-latest}
    expose:
      - '7117'
    volumes:
      - api-cache:/cache
    environment:
      - JAVA_TOOL_OPTIONS="-Xmx4g -Dquarkus.http.cors.origins=https://${FIRST_DOMAIN:-demo.deephaven.app}"

  grpc-api:
    image: ${REPO:-ghcr.io/deephaven}/grpc-api:${VERSION:-latest}
    expose:
      - '8888'
    volumes:
      - ./data:/data
      - api-cache:/cache
      - /etc/ssl/dh:/etc/ssl/dh
      - /etc/ssl/internal:/etc/ssl/internal
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx4g -Ddeephaven.console.type=${TYPE:-python}
      - DH_TLS_CHAIN=/etc/ssl/internal/tls.crt
      - DH_TLS_KEY=/etc/ssl/internal/tls.key.pk8

  web:
    image: ${REPO:-ghcr.io/deephaven}/web:${VERSION:-latest}
    expose:
      - '8080'
    volumes:
      - ./data:/data
      - /etc/ssl/dh:/etc/ssl/dh
      - web-tmp:/tmp

  envoy:
    image: ${REPO:-ghcr.io/deephaven}/envoy:${VERSION:-latest}
    depends_on:
      - web
      - grpc-api
    ports:
      - "${PORT:-10000}:10000"
    volumes:
      - /etc/ssl/dh:/etc/ssl/dh
      - /etc/ssl/internal:/etc/ssl/internal

  examples:
    # image: ${REPO:-ghcr.io/deephaven}/examples
    # this one isn't deployed to the gcloud docker repo
    image: ghcr.io/deephaven/examples
    volumes:
      - ./data:/data
    command: initialize

volumes:
    web-tmp:
    api-cache:

EOF
# End default docker-compose.yml

# Pull certificates out of kubectl
test -s "$DH_SSL_DIR/tls.key" ||
    { kubectl get secret deephaven-app-cert -o go-template='{{index .data "tls.key" | base64decode}}' | sudo tee "$DH_SSL_DIR/tls.key" > /dev/null ; }
test -s "$DH_SSL_DIR/tls.key.pk8" ||
    { sudo openssl pkcs8 -topk8 -nocrypt -in "$DH_SSL_DIR/tls.key" -out "$DH_SSL_DIR/tls.key.pk8" ; sudo chmod a+r /etc/ssl/dh/tls.key.pk8 ; }
# TODO: also use openssl checkend on tls.crt...
{ test -s "$DH_SSL_DIR/tls.crt" ; } ||
    { kubectl get secret deephaven-app-cert -o go-template='{{index .data "tls.crt" | base64decode}}' | sudo tee "$DH_SSL_DIR/tls.crt" > /dev/null ; }
{ test -s "$DH_SSL_DIR/ca.crt" ; } ||
    { kubectl get secret deephaven-app-ca -o go-template='{{index .data "ca.crt" | base64decode}}' | sudo tee "$DH_SSL_DIR/ca.crt" > /dev/null ; }

# Compute some default variables
VERSION=${VERSION:-0.0.4}
TYPE=${TYPE:-python}
ZONE=${ZONE:-us-central1}
PROJECT_ID=${PROJECT_ID:-deephaven-oss}
REPO=${REPO:-${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven}

# Create an .env file for docker-compose to read our variables from
echo "
VERSION=$VERSION
REPO=$REPO
TYPE=$TYPE
" > "$DH_DIR/.env"

# Record any of the values-for-docker-compose that are currently set onto filesystem
test -n "$VERSION" && echo "$VERSION" > "$DH_DIR/VERSION" || true
test -n "$TYPE" && echo "$TYPE" > "$DH_DIR/TYPE" || true
test -n "$REPO" && echo "$REPO" > "$DH_DIR/REPO" || true

export VERSION
export REPO
export TYPE
#
## Now, download the images that the actual vm runtime will use:
#docker-compose pull || {
#    # Authorize this machine to see the demo system docker repo
#    log "Failed to run docker-compose pull, attempting to use gcloud auth configure-docker to get credentials to the docker registry"
#    # Retry the pull
#    docker-compose pull
#}

REPO_ROOT="$(echo "$REPO" | cut -d "/" -f1 )"
gcloud auth configure-docker "${REPO_ROOT}" -q

# Run docker-compose pull in a new shell, so it uses newly-created docker group
bash -c "cd $DH_DIR ; docker-compose pull"

# Copy certificates generated by gen-certs.sh into our production locations
sudo mkdir -p /etc/ssl/internal
AUTH_DIR=${AUTH_DIR:-/tmp/scratch}
sudo mv ${AUTH_DIR}/tls.crt /etc/ssl/internal
sudo mv ${AUTH_DIR}/tls.key /etc/ssl/internal
sudo mv ${AUTH_DIR}/tls.key.pk8 /etc/ssl/internal
sudo mv ${AUTH_DIR}/ca.crt /etc/ssl/internal
sudo rm -rf ${AUTH_DIR}


# Create, but don't yet enable our systemd unit
# Create a systemd service that autostarts & manages a docker-compose instance in the current directory
SERVICENAME=${SERVICENAME:-dh}
log "Creating systemd service in /etc/systemd/system/${SERVICENAME}.service"
# Create systemd service file
sudo cat >/etc/systemd/system/$SERVICENAME.service <<EOF
[Unit]
Description=$SERVICENAME
Requires=docker.service
After=docker.service
[Service]
Restart=always
User=root
Group=docker
WorkingDirectory=$DH_DIR
# Shutdown container (if running) when unit is started
ExecStartPre=$(which docker-compose) -f docker-compose.yml down
# Start container when unit is started
ExecStart=$(which docker-compose) -f docker-compose.yml up
# Stop container when unit is stopped
ExecStop=$(which docker-compose) -f docker-compose.yml down
[Install]
WantedBy=multi-user.target
EOF


log "Enabling & starting $SERVICENAME"
# Autostart systemd service
sudo systemctl enable $SERVICENAME.service
# Start systemd service now, which will run docker-compose up -d
sudo systemctl start $SERVICENAME.service

while ! curl -k https://localhost:10000/ide/ &> /dev/null; do
    log "Waiting for server to respond"
    sleep 1
done

# setup iptables to redirect 443 and 80 to envoy
if !systemctl is-enabled netfilter-persistent; then
    # netfilter-persistent needs to be installed first for it to work
    sudo apt-get -yq install netfilter-persistent
fi
if !systemctl is-enabled iptables-persistent; then
    sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install iptables-persistent
fi
sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port 10000
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 10000
sudo mkdir -p /etc/iptables
sudo /sbin/iptables-save | sudo tee /etc/iptables/rules.v4 > /dev/null
sudo ip6tables-save | sudo tee /etc/iptables/rules.v6 > /dev/null
sudo netfilter-persistent save

log "System setup complete!"