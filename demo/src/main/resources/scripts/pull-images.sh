# Compute some default variables
export VERSION="${VERSION:-0.5.0}"
export TYPE="${TYPE:-python}"
export ZONE="${ZONE:-us-central1}"
export PROJECT_ID="${PROJECT_ID:-deephaven-oss}"
export REPO="${REPO:-${ZONE}-docker.pkg.dev/${PROJECT_ID}/deephaven}"
export DH_DIR="${DH_DIR:-/dh}"
# Create an .env file for docker-compose to read our variables from.
echo "
VERSION=$VERSION
REPO=$REPO
TYPE=$TYPE
DOMAIN=${DOMAIN:-${FIRST_DOMAIN:-demo.deephaven.app}}
" > "$DH_DIR/.env"

# Run docker-compose pull in a new shell, so it uses newly-created docker group
REPO_ROOT="$(echo "$REPO" | cut -d "/" -f1 )"
bash -c "cd $DH_DIR ; docker-compose pull" || {
    log "Unable to pull images; attempting to reauthenticate with gcloud"
    gcloud auth configure-docker "${REPO_ROOT}" -q
    bash -c "cd $DH_DIR ; docker-compose pull"
}