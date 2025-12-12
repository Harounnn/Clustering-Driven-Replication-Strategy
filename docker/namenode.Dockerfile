FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

RUN if [ -f /etc/apt/sources.list ]; then \
      sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list || true ; \
      sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian|g' /etc/apt/sources.list || true ; \
    fi && \
    printf 'Acquire::Check-Valid-Until "0";\n' > /etc/apt/apt.conf.d/99no-check-valid-until && \
    apt-get update -o Acquire::Check-Valid-Until=false && \
    apt-get install -y --no-install-recommends python3 python3-pip ca-certificates && \
    ln -s /usr/bin/python3 /usr/bin/python || true && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/synth-code
USER root
