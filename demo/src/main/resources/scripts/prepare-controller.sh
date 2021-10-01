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
      - JAVA_TOOL_OPTIONS=-Xmx4g

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

# Make sure docker is in a functional state (it won't be if we just created docker user)
#cd "$DH_DIR"
#log "Printing docker info:"
#docker info || {
#    log "Failed to run: docker info"
#    log "THIS IS NORMAL IF we just created the docker user"
#    log "Starting a new session using: newgrp docker"
#
#    echo "$DATA_DIR" > "$DH_DIR/data.link"
#    echo "$DH_SSL_DIR" > "$DH_DIR/ssl.dir"
#
#    # THIS DOES NOT WORK! WE'LL LOSE THE SHELL IF THIS HAPPENS. WE NEED TO RE-RUN THE WHOLE THING IF WE NEEDED TO MUCK WITH GROUPS
#    newgrp docker
#    # Have to reset our variables after starting a new shell with newgrp (only matters when first creating snapshot, new VMs start w/ fresh groups)
#    DH_DIR="$(pwd)"
#    DATA_DIR="$(< "$DH_DIR/data.link")"
#    DH_SSL_DIR="$(< "$DH_DIR/ssl.dir")"
#    rm "$DH_DIR/data.link"
#    rm "$DH_DIR/ssl.dir"
#}
#
## Pull VERSION, TYPE and REPO variables off of filesystem, in case we've reset the shell session
#test -n "$TYPE" || { test -f "$DH_DIR/TYPE" && TYPE="$(< "$DH_DIR/TYPE")" ; } || true
#test -n "$VERSION" || { test -f "$DH_DIR/VERSION" && VERSION="$(< "$DH_DIR/VERSION")" ; } || true
#test -n "$REPO" || { test -f "$DH_DIR/REPO" && REPO="$(< "$DH_DIR/REPO")" ; } || true

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

# Prepare some certificates

AUTH_DIR="${AUTH_DIR:-/tmp/scratch}"
mkdir -p "$AUTH_DIR"
NEW_CERT_DIR="${NEW_CERT_DIR:-$AUTH_DIR}"
mkdir -p "$NEW_CERT_DIR"
CA_INDEX="${CA_INDEX:-$NEW_CERT_DIR/index.txt}"
CA_SERIAL="${CA_SERIAL:-$NEW_CERT_DIR/serial.txt}"

KEY_FILE="${KEY_FILE:-$AUTH_DIR/tls.key}"
CERT_CONF_FILE="${CERT_CONF_FILE:-$AUTH_DIR/tls.conf}"
CA_CONF_FILE="${CA_CONF_FILE:-$AUTH_DIR/ca.conf}"
CSR_FILE="${CSR_FILE:-$AUTH_DIR/tls.csr}"
CERT_FILE="${CERT_FILE:-$AUTH_DIR/tls.crt}"
CERT_TTL="${CERT_TTL:-365}"
CERT_CA_FILE="${CERT_CA_FILE:-$AUTH_DIR/ca.crt}"
KEY_CA_FILE="${KEY_CA_FILE:-$AUTH_DIR/ca.key}"
KEY_LENGTH="${KEY_LENGTH:-4096}"
REGION=us-central1

MY_POD_IP="${MY_POD_IP:-}"
IP_LIST="${IP_LIST:-127.0.0.1,::1,$MY_POD_IP}"
DOMAIN_LIST="${DOMAIN_LIST:-demo.deephaven.app,*.demo.deephaven.app,localhost}"
#Pick the first domain out of the list
# The `|| [ -n` bit ensures we handle when trailing newline is missing
while read -r DOMAIN || [ -n "$DOMAIN" ]; do
  [ -z "$DOMAIN" ] && continue
  FIRST_DOMAIN="$DOMAIN"
  break
  # ugly bashism for "split on space, comma or newline": use sed to normalize "," and " " to "\n"
done < <(echo "${DOMAIN_LIST}" | sed -e 's/,/\n/g' -e 's/ /\n/g')
if [ -z "$FIRST_DOMAIN" ]; then
  FIRST_DOMAIN="${CERT_COMMON_NAME:-}"
fi
if [ -z "$FIRST_DOMAIN" ]; then
  echo "No valid DNS names found, and no CERT_COMMON_NAME specified"
  exit 95
fi

CERT_EMAIL="${CERT_EMAIL:-operations@deephaven.io}"
CERT_COUNTRY="${CERT_COUNTRY:-US}"
CERT_STATE="${CERT_STATE:-New York}"
CERT_CITY="${CERT_CITY:-New York}"
CERT_ORG_NAME="${CERT_ORG_NAME:-Deephaven}"
CERT_ORG_UNIT="${CERT_ORG_UNIT:-devops}"
CERT_COMMON_NAME="${CERT_COMMON_NAME:-$FIRST_DOMAIN}"
CERT_SUBJECT="/emailAddress=${CERT_EMAIL}/C=${CERT_COUNTRY}/ST=${CERT_STATE}/L=${CERT_CITY}/O=${CERT_ORG_NAME}/OU=${CERT_ORG_UNIT}/CN=${CERT_COMMON_NAME}"

# Make sure openssl is installed, and version in logs in case it's old (tested on 1.1.1)
openssl version

cat << EOF > "$CERT_CONF_FILE"
[req]
default_bits                   = $KEY_LENGTH
req_extensions                 = extension_requirements
distinguished_name             = dn_requirements

[extension_requirements]
basicConstraints               = CA:FALSE
keyUsage                       = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName                 = @sans_list

[dn_requirements]
countryName                    = Country Name (2 letter code)
countryName_default            = $CERT_COUNTRY
stateOrProvinceName            = State or Province Name (full name)
stateOrProvinceName_default    = $CERT_STATE
localityName                   = Locality Name (eg, city)
localityName_default           = $CERT_CITY
0.organizationName             = Organization Name (eg, company)
0.organizationName_default     = $CERT_ORG_NAME
organizationalUnitName         = Organizational Unit Name (eg, section)
organizationalUnitName_default = $CERT_ORG_UNIT
commonName                     = Common Name (e.g. server FQDN or YOUR name)
commonName_default             = $CERT_COMMON_NAME
emailAddress                   = Email Address
emailAddress_default           = $CERT_EMAIL

[sans_list]
EOF

# Now, append all domains to the [sans_list] of the conf file
FIRST_DOMAIN=
n=0
while read -r DOMAIN || [ -n "$DOMAIN" ]; do
  [ -z "$DOMAIN" ] && continue
  n=$((n+1))
  echo "DNS.$n                     = $DOMAIN" |
    tee -a "$CERT_CONF_FILE" >/dev/null
done < <(echo "${DOMAIN_LIST}" | sed -e 's/,/\n/g' -e 's/ /\n/g')
# Then, insert all IP addresses
n=0
while read -r DOMAIN || [ -n "$DOMAIN" ]; do
  [ -z "$DOMAIN" ] && continue
  n=$((n+1))
  echo "IP.$n                      = $DOMAIN" |
    tee -a "$CERT_CONF_FILE" >/dev/null
done < <(echo "${IP_LIST}" | sed -e 's/,/\n/g' -e 's/ /\n/g')

# GKE logs often split lines and render reverse order, so some whitespace for scanning logs can help
echo ""
echo ""
echo "certificate conf file:"
echo ""
echo ""
cat "$CERT_CONF_FILE"

if [ -f "$CERT_CA_FILE" ]; then
  if [ ! -f "$KEY_CA_FILE" ]; then
    echo "Found CERT_CA_FILE $CERT_CA_FILE but no KEY_CA_FILE set"
    exit 95
  fi
  # TODO: verify the ca crt+key are valid, and has necessary extensions CA:true, keyCertSign, cRLSign
else
  # ensure ca key file first
  if [ -f "$KEY_CA_FILE" ]; then
    # not sure if we need to verify ca is also rsa-or-ecdsa
    :
  else
    openssl genrsa -out "$KEY_CA_FILE" "$KEY_LENGTH"
  fi

  # create ca conf file, so we can make a ca cert
  cat <<EOF >"$CA_CONF_FILE"
HOME            = .
RANDFILE        = $NEW_CERT_DIR/.rnd

####################################################################
[ ca ]
default_ca    = CA_default      # The default ca section

[ CA_default ]

default_days     = 1000         # How long to certify for
default_crl_days = 90           # How long before next CRL
default_md       = sha256       # Use public key default MD
preserve         = no           # Keep passed DN ordering

x509_extensions = ca_extensions # The extensions to add to the cert

email_in_dn     = no            # Don't concat the email in the DN
copy_extensions = copy          # Required to copy SANs from CSR to cert

base_dir         = $AUTH_DIR
certificate      = $CERT_CA_FILE   # The CA certificate
private_key      = $KEY_CA_FILE    # The CA private key
new_certs_dir    = $NEW_CERT_DIR        # Location for new certs after signing
database         = $CA_INDEX    # Database index file
serial           = $CA_SERIAL   # The current serial number

unique_subject = no  # Set to 'no' to allow creation of
                     # several certificates with same subject.

####################################################################
[ req ]
default_bits       = ${KEY_LENGTH}
default_keyfile    = $(basename "$KEY_CA_FILE")
distinguished_name = ca_distinguished_name
x509_extensions    = ca_extensions
string_mask        = utf8only

####################################################################
[ ca_distinguished_name ]
countryName                    = Country Name (2 letter code)
countryName_default            = $CERT_COUNTRY
stateOrProvinceName            = State or Province Name (full name)
stateOrProvinceName_default    = $CERT_STATE
localityName                   = Locality Name (eg, city)
localityName_default           = $CERT_CITY
organizationName               = Organization Name (eg, company)
organizationName_default       = $CERT_ORG_NAME
organizationalUnitName         = Organizational Unit (eg, division)
organizationalUnitName_default = $CERT_ORG_UNIT
commonName                     = Common Name (e.g. server FQDN or YOUR name)
commonName_default             = $CERT_COMMON_NAME
emailAddress         = Email Address
emailAddress_default = $CERT_EMAIL

####################################################################
[ ca_extensions ]

subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always, issuer
basicConstraints       = critical, CA:true
keyUsage               = keyCertSign, cRLSign

####################################################################
[ signing_policy ]
countryName            = optional
stateOrProvinceName    = optional
localityName           = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

####################################################################
[ signing_req ]
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer
basicConstraints       = CA:FALSE
keyUsage               = digitalSignature, keyEncipherment


EOF

    echo ""
    echo ""
    echo "ca conf file:"
    echo ""
    echo ""
    cat "$CA_CONF_FILE"

    # If serial file does not exist or is empty, prime it w/ 01
    if [[ ! -s "$CA_SERIAL" ]]; then
         echo '01' > "$CA_SERIAL"
    fi
    if [[ ! -f "$CA_INDEX" ]]; then
        touch "$CA_INDEX"
    fi

    # Create the CA!
    openssl req -x509 \
        -config "${CA_CONF_FILE}" \
        -key "${KEY_CA_FILE}" \
        -sha256 -nodes \
        -out "$CERT_CA_FILE" \
        -subj "$CERT_SUBJECT"

fi

# https://cloud.google.com/load-balancing/docs/ssl-certificates/self-managed-certs#create-key-and-cert
# create a keyfile (google very specific about format of keys)
if [ -f "$KEY_FILE" ]; then
  # TODO: validate existing key is either RSA-2048 or ECDSA P-256.
  if openssl rsa -in "${KEY_FILE}" -text -noout &>/dev/null; then
      # key is rsa
      :
  elif openssl ec -in "${KEY_FILE}" -text -noout &>/dev/null; then
      # key is ecdsa
      :
  else
      echo "$KEY_FILE is not a valid rsa or ecdsa key!"
      exit 99
  fi
else
  # gke requires 2048, but grpc likes 4096, so we parameterize (default 4096)
  openssl genrsa -out "$KEY_FILE" "${KEY_LENGTH}"
fi

if [ ! -s "$CSR_FILE" ]; then
    # create a csr
    echo "Creating new certificate signing request: $CSR_FILE"
    openssl req -new \
      -key "$KEY_FILE" \
      -out "$CSR_FILE" \
      -config "$CERT_CONF_FILE" \
      -subj "$CERT_SUBJECT"
fi

# optional: view the csr
openssl req -text -noout -verify -in "$CSR_FILE"

# create self-signed certificate from the csr (this is fine for intra-cluster communication)
openssl x509 -req \
  -in "$CSR_FILE" \
  -out "$CERT_FILE" \
  -CA "$CERT_CA_FILE" \
  -CAkey "$KEY_CA_FILE" \
  -CAserial "$CA_SERIAL" \
  -days "${CERT_TTL}" \
  -extensions extension_requirements \
  -extfile "${CERT_CONF_FILE}"
# optional: view certificate details
openssl x509 -in "$CERT_FILE" -text -noout
chmod a+r "$CERT_FILE"


# For grpc, we need to convert the rsa private key to pkcs8
openssl pkcs8 -topk8 -nocrypt -in "${KEY_FILE}" -out "${KEY_FILE}.pk8"
chmod a+r "${KEY_FILE}.pk8"
# gross... figure out something that makes envoy happy
chmod a+r "${KEY_FILE}"

sudo mkdir -p /etc/ssl/internal
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


# Extra stuff just for controller


cat << EOF > /bin/dh_update_ctrl
cd /dh &&
 {
    sudo docker-compose pull ||
    {
        gcloud auth configure-docker "${REPO_ROOT}" -q ;
        sudo docker-compose pull
    }
 }&&
 sudo systemctl start dh &&
 sudo rm -rf /deployments &&
 while ! sudo docker ps | grep dh_demo-server_1; do sleep 1 ; done &&
 sudo docker cp dh_demo-server_1:/deployments /deployments &&
 sudo systemctl stop dh &&
 echo "Done updating controller" &&
 cd -
EOF
chmod a+x /bin/dh_update_ctrl

cat << EOF > /bin/dh_start_ctrl
cd /deployments &&
  sudo JAVA_OPTIONS="-Dquarkus.http.host=0.0.0.0
                     -Dquarkus.http.port=7117
                     -Djava.util.logging.manager=org.jboss.logmanager.LogManager
                     -Dquarkus.http.access-log.enabled=true
                     -Dquarkus.http.cors.origins=https://${FIRST_DOMAIN:-demo.deephaven.app}
                     -Dquarkus.ssl.native=false" ./run-java.sh &&
  cd -

EOF
chmod a+x /bin/dh_start_ctrl

cat << EOF > /bin/dh_start_envoy
sudo envoy -c /etc/envoy/envoy.yaml
EOF
chmod a+x /bin/dh_start_envoy

cat << EOF > /etc/envoy/envoy.yaml
admin:
  # access_log_path: /dev/stdout
  access_log_path: /tmp/admin_access.log
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9090
static_resources:
  listeners:
    - name: listener_0
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 10000
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                access_log:
                  - name: envoy.access_loggers.file
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
                      path: "/dev/stdout"
                codec_type: AUTO
                stat_prefix: ingress_https
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: reverse_proxy
                      domains: ["*"]
                      routes:
                        - match: # Call to / goes to the landing page
                            prefix: "/"
                          route: { cluster: control, timeout: 120s }
                common_http_protocol_options:
                  max_stream_duration: 120s
                http_filters:
                  - name: envoy.filters.http.health_check
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.health_check.v3.HealthCheck
                      pass_through_mode: false
                      headers:
                        - name: ":path"
                          exact_match: "/healthz"
                        - name: "x-envoy-livenessprobe"
                          exact_match: "healthz"
                  - name: envoy.filters.http.grpc_web
                  - name: envoy.filters.http.router
          transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              session_timeout: 120s
              common_tls_context:
                alpn_protocols: ["h2","http/1.1"]
                tls_certificates:
                  - certificate_chain:
                      filename: /etc/ssl/dh/tls.crt
                    private_key:
                      filename: /etc/ssl/dh/tls.key
  clusters:
    - name: control
      connect_timeout: 10s
      http_protocol_options:
        max_stream_duration: 120s
      type: LOGICAL_DNS
      lb_policy: ROUND_ROBIN
      http_protocol_options: {}
      load_assignment:
        cluster_name: control
        endpoints:
          - lb_endpoints:
              - endpoint:
                  hostname: control
                  address:
                    socket_address:
                      address: 0.0.0.0
                      port_value: 7117

EOF

dh_update_ctrl
dh_start_ctrl
dh_start_envoy


sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port 10000
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 10000
sudo mkdir -p /etc/iptables
sudo /sbin/iptables-save | sudo tee /etc/iptables/rules.v4 > /dev/null
sudo /sbin/ip6tables-save | sudo tee /etc/iptables/rules.v6 > /dev/null
sudo netfilter-persistent save

log "System setup complete!"
