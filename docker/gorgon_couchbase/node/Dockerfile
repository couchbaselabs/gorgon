FROM ubuntu:24.04

ARG CB_VERSION
ENV LANG=C.UTF-8

RUN apt-get -y update && apt-get -y install \
    curl golang-go iptables netcat-openbsd procps python3 sudo tar unzip wget

RUN wget https://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-noarch.deb \
    && dpkg -i couchbase-release-1.0-noarch.deb \
    && rm couchbase-release-1.0-noarch.deb \
    && apt-get -y update \
    && apt-get -y install couchbase-server${CB_VERSION:+=$CB_VERSION}

RUN mkdir /root/deps
ADD deps.tgz /root/deps
WORKDIR /root/deps/src/gorgon_couchbase
RUN go mod download

ADD src.tgz /
WORKDIR /src/gorgon_couchbase
RUN go build

WORKDIR /root
COPY init.sh run.sh /

ENTRYPOINT ["sh", "/init.sh"]
