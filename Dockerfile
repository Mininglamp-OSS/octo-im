# syntax=docker/dockerfile:1

# Compile frontends on the builder's native CPU for both image architectures.
FROM --platform=$BUILDPLATFORM node:22-bookworm AS frontend
WORKDIR /frontend
COPY demo/chatdemo/package.json demo/chatdemo/yarn.lock ./chatdemo/
RUN cd chatdemo && yarn install --frozen-lockfile --non-interactive
COPY demo/chatdemo ./chatdemo
RUN cd chatdemo && yarn build
COPY web/package.json web/yarn.lock ./web/
RUN cd web && yarn install --frozen-lockfile --non-interactive
COPY web ./web
RUN cd web && yarn build

# Keep the toolchain used by the validated fix1 build.
FROM --platform=$BUILDPLATFORM golang:1.23.0 AS build
ENV GOTOOLCHAIN=local CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /frontend/chatdemo/dist ./demo/chatdemo/dist
COPY --from=frontend /frontend/web/dist ./web/dist
ARG TARGETOS
ARG TARGETARCH
ARG SOURCE_REVISION=unknown
ARG SOURCE_DATE=0
ARG SOURCE_VERSION=dev
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-w -extldflags '-static' -X main.Commit=${SOURCE_REVISION} -X main.CommitDate=${SOURCE_DATE} -X main.Version=${SOURCE_VERSION} -X main.TreeState=clean" \
    -installsuffix cgo -o /out/app ./main.go && \
    go version -m /out/app > /out/buildinfo.txt

FROM alpine:3.23.4 AS prod
RUN apk add --no-cache ca-certificates tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone
WORKDIR /home
COPY --from=build /out/app /home/app
COPY --from=build /out/buildinfo.txt /home/buildinfo.txt
COPY config/wk.yaml /root/wukongim/wk.yaml
ENTRYPOINT ["/home/app","--config=/root/wukongim/wk.yaml","--ignoreMissingConfig=true"]
