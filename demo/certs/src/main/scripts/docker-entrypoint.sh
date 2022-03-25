#!/bin/sh

SVC_ACT_KEY="${SVC_ACT_KEY:-/svc/deephaven-svc-act.json}"

echo "Beginning wildcard cert generation using service account $SVC_ACT_KEY"

set -x

NAMESPACE=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace) 2> /dev/null ||
  NAMESPACE=

if [ "$DEBUG" = true ]; then

    echo
    echo "Dumping env"
    env
    # flush stdout/stderr and give some whitespace around big variable list
    echo
    echo 2> /dev/null

    K8_PROJECT_ID="${K8_PROJECT_ID:-"$(curl -q "http://metadata.google.internal/computeMetadata/v1/project/project-id" -H "Metadata-Flavor: Google")"}"
    echo "K8_PROJECT_ID: ${K8_PROJECT_ID}"
    K8_SVC_ACT=${K8_SVC_ACT:-$(
        echo "You must set K8_SVC_ACT when setting DEBUG=true"
        echo "Valid service accounts:"
        curl "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/" -H "Metadata-Flavor: Google"
        exit 101
    )}
    META_URL="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/${K8_PROJECT_ID}.svc.id.goog"
    echo "Debugging running service account $K8_SVC_ACT using $META_URL"
    if curl "$META_URL/identity" -H "Metadata-Flavor: Google"; then
      echo
      echo "Printing detailed service account information"
      curl "${META_URL}/email" -H "Metadata-Flavor: Google"
      curl "${META_URL}/scopes" -H "Metadata-Flavor: Google"
      curl "${META_URL}/token" -H "Metadata-Flavor: Google"
      echo "All keys in $K8_SVC_ACT"
      curl -q "${META_URL}/" -H "Metadata-Flavor: Google"
    else
      echo "Unable to query $K8_SVC_ACT, check logs above for errors" >&2
    fi
    echo "Listing location where we expect kubernetes service account (or, at least, a usable ca.crt and token file)"
    ls -la /var/run/secrets/kubernetes.io/serviceaccount/
fi

[ -n "$DOMAIN" ] &&
  DOMAINS="${DOMAINS:-$DOMAIN,*.$DOMAIN}"

if [ -z "$EMAIL" ] || [ -z "$DOMAINS" ] || [ -z "$SECRET" ]; then
	echo "EMAIL, DOMAINS, SECRET env vars required"
	env
	exit 1
fi

# We only need service account json keys when the kubernetes service account does not have a proper workload identity setup.
# You can test the workload identity setup by setting DEBUG=true (in Dockerfile or k8 yaml) and viewing logs.
[ -f "$SVC_ACT_KEY" ] && MAYBE_SVC_JSON="--dns-google-credentials ${SVC_ACT_KEY}" || MAYBE_SVC_JSON=

cd $HOME
certbot certonly \
  --email "$EMAIL" \
  --agree-tos \
  --no-eff-email \
  --debug \
  --noninteractive \
  --dns-google \
  --dns-google-propagation-seconds 60 \
  ${MAYBE_SVC_JSON} \
  -d "$DOMAINS"
# --dns-google-credentials, above, is hacky, but needed:
# We need a service account json, SVC_ACT_KEY, to be able to run locally even though kubernetes should have service account provide access...
# unfortunately, when running in autopilot mode, we can't use hostNetwork, which is where the service account gets its auth

# Find where the cert lives
CERTPATH=/etc/letsencrypt/live/$(echo "${DOMAINS//\*./}" | cut -f1 -d',')

ls $CERTPATH || exit 1

# Copy cert contents into our json patch template
cat /rx/secret-patch-template.json | \
	sed "s/NAMESPACE/${NAMESPACE}/" | \
	sed "s/NAME/${SECRET}/" | \
	sed "s/TLSCERT/$(cat ${CERTPATH}/fullchain.pem | base64 | tr -d '\n')/" | \
	sed "s/TLSKEY/$(cat ${CERTPATH}/privkey.pem |  base64 | tr -d '\n')/" \
	> /tmp/secret-patch.json

# Only list the patch, don't render it to logs: it contains private key now!
ls /tmp/secret-patch.json || exit 1

# update secret, rely on kubernetes service account running us to have secrets + patch permission.
curl -v --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" -k -v -XPATCH  -H "Accept: application/json, */*" -H "Content-Type: application/strategic-merge-patch+json" -d @/tmp/secret-patch.json https://kubernetes/api/v1/namespaces/${NAMESPACE}/secrets/${SECRET} || {
    echo 'Unable to use service account token to update secrets'
    echo 'check your RoleBinding has apiGroups: [""], resources: ["secrets"], verbs: ["patch"] '
    exit 121
    # TODO: get the secret and if it doesn't exist, try to create it? (maybe just blindly try to POST instead of requiring GET permission)
}
