#FROM golang:1.13.12 AS builder
#MAINTAINER junedayday "junedayday@gmail.com"
#WORKDIR /app
#COPY . /app
#
#RUN git clone https://github.com/edenhill/librdkafka.git \
#&& cd librdkafka \
#&& ./configure \
#&& make \
#&& make install \
#&& cd .. \
#&& go build --tags static ./internal/binlogger
#
## lighter image
#FROM debian AS binlogger
#WORKDIR /app
#COPY --from=builder /app/binlogger .
#
#ENTRYPOINT ["/app/binlogger","-y=/app/binlogger.yaml","--log_dir=/app/log"]

FROM golang:1.13.12 AS builder2
MAINTAINER junedayday "junedayday@gmail.com"
WORKDIR /app
COPY . /app

RUN go build ./internal/binlogsyncer

FROM debian AS binlogsyncer
WORKDIR /app
COPY --from=builder2 /app/binlogsyncer .

ENTRYPOINT ["/app/binlogsyncer","-y=/app/binlogsyncer.yaml","--log_dir=/app/log"]

