FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN for d in ordersd inventoryd paymentd; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$d ./cmd/$d || exit 1; \
    done

# One image for the three daemons: pick one with the entrypoint.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ordersd /out/inventoryd /out/paymentd /
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/ordersd"]
