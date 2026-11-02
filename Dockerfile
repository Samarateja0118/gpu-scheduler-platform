# One Dockerfile for every binary: docker build --build-arg CMD=scheduler .
ARG BUILDER_IMAGE=golang:1.22-alpine
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM ${BUILDER_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=scheduler
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/app /app
USER 65532:65532
ENTRYPOINT ["/app"]
