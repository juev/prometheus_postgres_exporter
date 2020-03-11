FROM golang:1.13.7 AS build

WORKDIR /go/src/prometheus_postgres_exporter
COPY . .
RUN go get -d -v

ARG VERSION
ENV VERSION ${VERSION:-0.1.0}

ENV PKG_CONFIG_PATH /go/src/prometheus_postgres_exporter
ENV GOOS            linux

RUN go build -v -ldflags "-X main.Version=${VERSION} -s -w"

# new stage
FROM scratch
LABEL authors="Denis Evsyukov"
LABEL maintainer="Denis Evsyukov <denis@evsyukov.org>"

ENV VERSION ${VERSION:-0.1.0}

COPY --from=build /go/src/prometheus_postgres_exporter/prometheus_postgres_exporter /prometheus_postgres_exporter

EXPOSE 9102

ENTRYPOINT ["/prometheus_postgres_exporter"]