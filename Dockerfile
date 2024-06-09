FROM arm64v8/golang:1.22.4-alpine3.20 as build

WORKDIR /app

COPY go.mod go.sum ./

COPY main.go ./

COPY .env ./

COPY ciks.json ./

COPY domain ./domain

COPY adapter ./adapter

COPY service ./service

RUN mkdir test

RUN go build -o main main.go

FROM arm64v8/alpine:3.20

COPY --from=build /app/main /main

ENTRYPOINT [ "/main" ]
