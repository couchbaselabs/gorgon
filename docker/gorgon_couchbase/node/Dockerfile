FROM ubuntu:24.04

ENV LANG=C.UTF-8

RUN apt-get -qy update \
    && apt-get -qy install curl golang-go iptables netcat-openbsd procps python3 sudo tar unzip wget g++ cmake libfuse3-dev libfuse3-3 fuse3 git

#setting the directory for installation of lazyfs 
WORKDIR /opt

#cloning the repo (LazyFS is at dsrhaslab/lazyfs)
RUN git clone https://github.com/dsrhaslab/lazyfs.git lazyfs

#making changes to the lazyfs conf file
RUN sed -i 's/^#user_allow_other$/user_allow_other/' /etc/fuse.conf


WORKDIR /root
RUN wget https://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-noarch.deb \
    && dpkg -i ./couchbase-release-1.0-noarch.deb \
    && apt-get -qy update \
    && apt-get -qy install couchbase-server \
    && rm *.deb


#modifying the default.toml file to log lazyfs logs
RUN sed -i 's|^logfile=""|logfile="/tmp/lazyfs.log"|' /opt/lazyfs/lazyfs/config/default.toml
RUN sed -i 's|log_all_operations=false|log_all_operations=true|' /opt/lazyfs/lazyfs/config/default.toml
RUN sed -i 's/^#fifo_path_completed=/fifo_path_completed=/' /opt/lazyfs/lazyfs/config/default.toml


#running the build commands for lazyfs 
WORKDIR /opt/lazyfs
RUN cd libs/libpcache && ./build.sh && cd -
RUN cd lazyfs && ./build.sh && cd -

#creating the mount point and the actual disk space for lazyfs 
WORKDIR /tmp
RUN mkdir lazyfs.mnt 
RUN mkdir lazyfs.root

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
