## Building of the image

```shell
mvn clean assembly:assembly
docker build -t weather-station-mock:1.0.0 .
```