# docker-maven-sample
This is a sample that explains how we can build & package a maven project using Docker Containers.

Container used for built: maven:3.6.0-jdk-11-slim

Container used for packaging (executable): openjdk:11-jre-slim

## Building of the image

```shell
docker build -t demo:latest .
```

## Run the image

```shell
docker run --rm -it demo:latest
```