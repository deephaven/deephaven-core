SERVICENAME="${SERVICENAME:-dh}"
DH_DIR="${DH_DIR:-/dh}"
PORT="${PORT:10000}"
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
ExecStart=$docker_compose -f docker-compose.yml up
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

log "Waiting for server to respond on port $PORT"
while ! curl -k https://localhost:$PORT/ide/ &> /dev/null; do
    echo -n '.'
    sleep 1
done
log "Server is responding on port $PORT; redirecting 443 and 80 to $PORT"

# setup iptables to redirect 443 and 80 to envoy
if ! systemctl is-enabled netfilter-persistent; then
    # netfilter-persistent needs to be installed first for it to work
    sudo apt-get -yq install netfilter-persistent
fi
if ! systemctl is-enabled iptables-persistent; then
    sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install iptables-persistent
fi
sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port "$PORT"
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port "$PORT"
sudo mkdir -p /etc/iptables
sudo /sbin/iptables-save | sudo tee /etc/iptables/rules.v4 > /dev/null
sudo ip6tables-save | sudo tee /etc/iptables/rules.v6 > /dev/null
sudo netfilter-persistent save

log "System setup of $(hostname) complete!"