FROM debian:buster-slim as java_base
ARG ZULU_REPO_VER=1.0.0-3
RUN set -eux; \
    if [ -f /etc/dpkg/dpkg.cfg.d/docker ]; then \
        # if this file exists, we're likely in "debian:xxx-slim", and locales are thus being excluded so we need to remove that exclusion (since we need locales)
        grep -q '/usr/share/locale' /etc/dpkg/dpkg.cfg.d/docker; \
        sed -ri '/\/usr\/share\/locale/d' /etc/dpkg/dpkg.cfg.d/docker; \
        ! grep -q '/usr/share/locale' /etc/dpkg/dpkg.cfg.d/docker; \
    fi; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install gnupg software-properties-common locales curl; \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8; \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9; \
    curl -sLO https://cdn.azul.com/zulu/bin/zulu-repo_${ZULU_REPO_VER}_all.deb; \
    dpkg -i zulu-repo_${ZULU_REPO_VER}_all.deb; \
    apt-get -qq update; \
    apt-get -qq -y dist-upgrade; \
    mkdir -p /usr/share/man/man1; \
    apt-get -qq -y --no-install-recommends install zulu11-jdk-headless; \
    apt-get -qq -y purge gnupg software-properties-common curl; \
    apt -y autoremove; \
    rm -rf /var/lib/apt/lists/* zulu-repo_${ZULU_REPO_VER}_all.deb
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
ENV JAVA_HOME=/usr/lib/jvm/zulu11-ca-amd64

FROM java_base
RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install \
      python3.7 \
      libpython3.7 \
      python3-pip \
      python3-setuptools \
      python3-wheel \
      ; \
    rm -rf /var/lib/apt/lists/*; \
    python3 -m pip install -q --no-cache-dir --upgrade pip; \
    python3 -m pip install -q --no-cache-dir --upgrade setuptools; \
    python3 -m pip install -q --no-cache-dir --upgrade wheel
