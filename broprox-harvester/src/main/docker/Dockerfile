FROM java:8

MAINTAINER National Library of Norway

WORKDIR /

RUN mkdir /workdir
VOLUME /workdir
ENV WORK_DIR=/workdir

EXPOSE 8080 9090

ADD broprox-harvester-dist.tar.gz /

CMD ["/broprox-harvester/bin/broprox-harvester"]
