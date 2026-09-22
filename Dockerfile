FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG COMMAND
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/service ./cmd/${COMMAND}

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]

