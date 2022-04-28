#!/bin/bash
set -x
set -e

CLUSTER_NAME="${CLUSTER_NAME:-dhce-auto}"
PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
REGION="${REGION:-us-central1}"
K8S_CONTEXT="${K8S_CONTEXT:-gke_"${PROJECT_ID}_$REGION_$CLUSTER_NAME"}"
K8S_NAMESPACE=${K8S_NAMESPACE:-$CLUSTER_NAME}
K8S_SVC_ACT=cert-issuer
GCE_SVC_ACT=cert-issuer
GCE_ROLE=cert_issuer_role

ROLE_ITEMS="${ROLE_ITEMS:-\
dns.changes.create,\
dns.changes.get,\
dns.managedZones.list,\
dns.resourceRecordSets.create,\
dns.resourceRecordSets.delete,\
dns.resourceRecordSets.list,\
dns.resourceRecordSets.update}"

# Create / update your cluster to use workload identity pool
#gcloud container clusters create "${CLUSTER_NAME}" --workload-pool="${PROJECT_ID}.svc.id.goog"
# If cluster already exists and didn’t have workload pool, you can just edit it:
#gcloud container clusters update "${CLUSTER_NAME}" --workload-pool="${PROJECT_ID}.svc.id.goog"

# Ensure the cert-issuer kubernetes service account is created
kubectl --context "$K8S_CONTEXT" --namespace "${K8S_NAMESPACE}" apply -f ./cert-wildcard-account.yaml

# Ensure the gcloud service account exists
gcloud --project "${PROJECT_ID}" iam service-accounts describe "${GCE_SVC_ACT}@${PROJECT_ID}.iam.gserviceaccount.com" || {
    gcloud --project "${PROJECT_ID}" iam service-accounts create "${GCE_SVC_ACT}"
}

function has_all_roles() {
    local cur_roles
    cur_roles="$(gcloud --project "${PROJECT_ID}" iam roles describe "${GCE_ROLE}" --project "${PROJECT_ID}")"
    ROLE=
    while read -r ROLE || [ -n "$ROLE" ]; do
        [ -z "$ROLE" ] && continue
        # use glob-matching to make sure google's response contains the roles we're asking for
        if [[ "$cur_roles" != *"$ROLE"* ]]; then
            return 1
        fi
    done <<< "${ROLE_ITEMS//,/
}"
    # weird broken-looking line and indentation above is deliberate, we're splitting ROLE_ITEMS by comma using ${VAR//,/\n}
    # we then feed the now-newline-delimited list to done <<< "list\nitems" which supplies our while read -r VAR loop
}
gcloud  --project "${PROJECT_ID}" iam roles describe "${GCE_ROLE}" --project "${PROJECT_ID}" && {
    if ! has_all_roles; then
        gcloud  --project "${PROJECT_ID}" iam roles update "${GCE_ROLE}" \
            --permissions="${ROLE_ITEMS}" --project "${PROJECT_ID}"
    fi
} || {
    gcloud  --project "${PROJECT_ID}" iam roles create "${GCE_ROLE}" \
        --title="${GCE_ROLE}" --description="A role containing all permissions needed to issue certificates" \
        --permissions="${ROLE_ITEMS}" \
        --stage=GA --project "${PROJECT_ID}"
}

# Bind the gce role to the gce service account
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member "serviceAccount:${GCE_SVC_ACT}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role "projects/${PROJECT_ID}/roles/${GCE_ROLE}"

# Bind the k8 service account to the gce service account
gcloud  --project "${PROJECT_ID}" iam service-accounts add-iam-policy-binding "${GCE_SVC_ACT}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${K8S_NAMESPACE}/${K8S_SVC_ACT}]"

# Annotate the kubernetes service account so it knows which gce service account it can use
kubectl annotate serviceaccount "${K8S_SVC_ACT}" \
    --context "$K8S_CONTEXT" \
    --namespace "${K8S_NAMESPACE}" \
    --overwrite=true \
   iam.gke.io/gcp-service-account="${GCE_SVC_ACT}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Done setting up service accounts"
