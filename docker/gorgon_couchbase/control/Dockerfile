FROM ubuntu:24.04

ENV LANG=C.UTF-8

RUN apt-get -y update && apt-get -y install \
    curl golang-go netcat-openbsd python3 sudo tar unzip wget

RUN mkdir /root/deps && mkdir /workloads
ADD deps.tgz /root/deps
WORKDIR /root/deps/src/gorgon_couchbase
RUN go mod download

ADD src.tgz /
WORKDIR /src/gorgon_couchbase
RUN go build

RUN mkdir /root/store

WORKDIR /root
COPY init.sh run.sh /
COPY workloads/ /workloads/

ENTRYPOINT ["sh", "/init.sh"]
