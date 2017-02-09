FROM java:8

MAINTAINER National Library of Norway

WORKDIR /

VOLUME /workdir

ENV WORK_DIR=/workdir

EXPOSE 8080

ADD broprox-frontier-dist.tar.gz /

CMD ["/broprox-frontier/bin/broprox-frontier"]
