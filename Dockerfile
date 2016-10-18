FROM golang:1.6-alpine

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV DOCKER_BUILDTAGS include_oss include_gcs

RUN set -ex \
    && apk add --no-cache make git

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
COPY cmd/registry/config-hdfs.yml /etc/docker/registry/config.yml

RUN go get github.com/colinmarc/hdfs
RUN make PREFIX=/go clean binaries

EXPOSE 5000
ENTRYPOINT ["registry"]
CMD ["serve", "/etc/docker/registry/config.yml"]
