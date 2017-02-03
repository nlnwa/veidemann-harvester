FROM java:8

MAINTAINER National Library of Norway

WORKDIR /

VOLUME /workdir
VOLUME /warcs

ENV WORK_DIR=/workdir \
    WARC_DIR=/warcs

EXPOSE 8080

ADD broprox-contentwriter-service-dist.tar.gz /

CMD ["/broprox-contentwriter-service/bin/broprox-contentwriter-service"]
