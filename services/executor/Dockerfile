FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY services ./services
RUN go build -o /out/executor ./services/executor

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/executor /usr/local/bin/executor
COPY config /app/config
EXPOSE 5001
ENTRYPOINT ["/usr/local/bin/executor"]
