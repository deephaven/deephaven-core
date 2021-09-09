set -o nounset
set -o errexit
set -o xtrace
set -o pipefail

# First, setup all our variables. Feel free to override any file location using these variables

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
DOMAIN_LIST="${DOMAIN_LIST:-demo.deephavencommunity.com,*.demo.deephavencommunity.com,localhost}"
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

if [[ -n "$MY_POD_IP" ]]; then
    exit 0
fi

if (( KEY_LENGTH != 2048 )); then
    echo "KEY_LENGTH = $KEY_LENGTH, but deployment to google requires 2048 bit keys"
    exit 0
fi

if [ "$DEPLOY_GOOGLE" = y ]; then
    PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
    GCLOUD_CERT_NAME="${GCLOUD_CERT_NAME:-dh-demo-cert}"
    # push certificate to gcloud (for load balancer to find)
    gcloud compute ssl-certificates delete \
        --project="${PROJECT_ID}" \
        -q \
        "${GCLOUD_CERT_NAME}" \
        2> /dev/null || true
    gcloud compute ssl-certificates create "${GCLOUD_CERT_NAME}" \
      --certificate="${CERT_FILE}" \
      --private-key="${KEY_FILE}" \
      --global

    gcloud compute ssl-certificates delete \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        -q \
        "${GCLOUD_CERT_NAME}-us-central" \
        2> /dev/null || true
    gcloud compute ssl-certificates create "${GCLOUD_CERT_NAME}-us-central" \
      --certificate="${CERT_FILE}" \
      --private-key="${KEY_FILE}" \
      --region "${REGION}"


    # push certificate to kubernetes (for containers to consume)
    kubectl delete secret "${GCLOUD_CERT_NAME}" 2> /dev/null || true
    kubectl create secret tls "${GCLOUD_CERT_NAME}" \
      --cert="$CERT_FILE" \
      --key="${KEY_FILE}"

    # hm... stash the ca+ca-key too?
    # ...if we do it, we should only do it when we created them
fi

