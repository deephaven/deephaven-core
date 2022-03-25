MY_UNAME="${MY_UNAME:-$(id -un)}"
# Make sure we meet system requirements
apt_needs=""
command -v docker || apt_needs=" $apt_needs docker.io"
command -v docker-compose || apt_needs=" $apt_needs pip python3-dev libffi-dev openssl gcc libc-dev cargo make"
command -v curl || apt_needs=" $apt_needs curl"
test -n "$apt_needs" && {
    log "$apt_needs not already installed, installing from apt"
    sudo apt update -y
    sudo apt install -y $apt_needs
    sudo apt clean
    command -v docker-compose || {
        log "docker-compose not installed, downloading 1.29.2 from github"
        sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/bin/docker-compose
        sudo chmod +x /usr/bin/docker-compose
    }
}

# Make sure docker group exists, and our running user is in it.
# Warning: this leaks root access to logged in user account
# we should instead use rootless docker: https://docs.docker.com/engine/security/rootless
getent group docker > /dev/null || sudo groupadd docker
getent group docker | grep -c -E "[:,]$MY_UNAME" || sudo usermod -aG docker $MY_UNAME
