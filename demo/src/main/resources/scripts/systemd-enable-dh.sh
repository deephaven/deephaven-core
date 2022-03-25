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
Group=docker
WorkingDirectory=$DH_DIR
# Shutdown container (if running) when unit is started
ExecStartPre=$docker_compose -f docker-compose.yml down
# Start container when unit is started
ExecStart=$docker_compose -f docker-compose.yml up --force-recreate
# Stop container when unit is stopped
ExecStop=$docker_compose -f docker-compose.yml down
[Install]
WantedBy=multi-user.target
EOF

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
done
log "Server $(hostname) is responding on port $PORT; redirecting 443 and 80 to $PORT"
set -o xtrace