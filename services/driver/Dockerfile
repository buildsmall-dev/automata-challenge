FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY services ./services
RUN go build -o /out/driver ./services/driver

FROM alpine:3.20
RUN adduser -D -u 10001 app \
 && mkdir /ledger && chown app:app /ledger
USER app
COPY --from=build /out/driver /usr/local/bin/driver
EXPOSE 7000
ENTRYPOINT ["/usr/local/bin/driver"]
