SERVICENAME="${SERVICENAME:-dh}"
DH_DIR="${DH_DIR:-/dh}"
PORT="${PORT:-10000}"
docker_compose="$(command -v docker-compose)"

# Create a systemd service that autostarts & manages a docker-compose instance in the current directory (default is /dh)
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
Group=root
SupplementaryGroups=docker
WorkingDirectory=$DH_DIR
# Forcibly restart container whenever unit is started
ExecStart=+$docker_compose -f docker-compose.yml up --force-recreate
# Dump logs before we stop
ExecStop=-/dh/grab_logs
# Stop container when unit is stopped
ExecStop=$docker_compose -f docker-compose.yml down
TimeoutSec=1min 30s
[Install]
WantedBy=multi-user.target
EOF

cat > /dh/grab_logs <<EOF
if (( $(id -u) != 0 )); then
    sudo /dh/grab_logs
    exit $?
fi
set -o errexit
mkdir -p /dh/logs
log_file="/dh/logs/dh-log-\$(date "+%Y%m%d%H%M%S").log"
$docker_compose logs > "\$log_file"
ln -nsf "\$log_file" /dh/logs/dh-log-current
EOF
chmod +x /dh/grab_logs

log "Enabling & starting $SERVICENAME"
# Autostart systemd service
sudo systemctl enable "${SERVICENAME}.service"
# Start systemd service now, which will run docker-compose up -d
sudo systemctl start "${SERVICENAME}.service"

set +o xtrace
log "Waiting for server $(hostname) to respond on port $PORT"
tries=600
while ! curl -k https://localhost:$PORT/ide/ &> /dev/null; do
    echo -n '.'
    sleep 1
    tries=$(( tries - 1 ))
    if (( tries = 0 )); then
        log "Waited more than 600 seconds for $PORT to respond on localhost; giving up"
        exit 106
    fi
done
log "Server $(hostname) is responding on port $PORT; redirecting 443 and 80 to $PORT"
set -o xtrace
