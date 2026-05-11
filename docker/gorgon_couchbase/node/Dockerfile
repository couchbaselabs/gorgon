FROM ubuntu:24.04

ENV LANG=C.UTF-8

RUN apt-get -y update && apt-get -y install \
    curl golang-go iptables netcat-openbsd procps python3 sudo tar unzip wget tshark

ARG CB_URL=""
RUN if [ -n "$CB_URL" ]; then \
        wget -q --show-progress --progress=bar:force "$CB_URL" -O couchbase-server.deb \
        && dpkg -i couchbase-server.deb \
        && rm couchbase-server.deb; \
    else \
        wget -q --show-progress --progress=bar:force https://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-noarch.deb \
        && dpkg -i couchbase-release-1.0-noarch.deb \
        && rm couchbase-release-1.0-noarch.deb \
        && apt-get -y update \
        && apt-get -y install couchbase-server; \
    fi

RUN mkdir /root/deps
RUN mkdir -p /root/store/cbcollects_and_captures
ADD deps.tgz /root/deps
WORKDIR /root/deps/src/gorgon_couchbase
RUN go mod download

ADD src.tgz /
WORKDIR /src/gorgon_couchbase
RUN go build

WORKDIR /root
COPY init.sh run.sh /

ENTRYPOINT ["sh", "/init.sh"]
