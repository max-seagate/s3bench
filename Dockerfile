FROM golang:latest

RUN go get github.com/aws/aws-sdk-go/aws github.com/jmespath/go-jmespath

COPY . /s3bench

WORKDIR /s3bench

RUN ./build.sh
