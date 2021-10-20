PORT="${PORT:-10000}"

# setup iptables to redirect 443 and 80 to envoy
if ! systemctl is-enabled netfilter-persistent; then
    # netfilter-persistent needs to be installed first for it to work
    sudo apt-get -yq install netfilter-persistent
fi
if ! systemctl is-enabled iptables-persistent; then
    sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install iptables-persistent
fi

sudo iptables -t nat -L | grep -q "${PORT}" || {
    log "Port 10000 redirect not setup, adding iptables rules"
    sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port "$PORT"
    sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port "$PORT"
    sudo mkdir -p /etc/iptables
    sudo /sbin/iptables-save | sudo tee /etc/iptables/rules.v4 > /dev/null
    sudo ip6tables-save | sudo tee /etc/iptables/rules.v6 > /dev/null
    sudo netfilter-persistent save
}

log "System setup of $(hostname) complete!"
# Do not change this message, it must be the very last thing in the log file
log "InitialDeephavenSetupComplete"