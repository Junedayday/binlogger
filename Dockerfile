FROM golang:1.13-alpine

MAINTAINER junedayday "junedayday@gmail.com"

WORKDIR /app

COPY . /app

RUN go build ./internal/binlogger \
&& rm -rf !\(binlogger\)

ENTRYPOINT ["/app/binlogger","--log_dir=/app/log"]

