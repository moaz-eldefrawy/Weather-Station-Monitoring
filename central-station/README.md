# Important Note
This project uses the weather-station-mock project as a dependency. Make sure you run `maven clean install` in weather-station-mock so that it's added to the local maven repo (where it can be referenced by the central-station's pom.xml)
## Building of the image

```shell
mvn clean assembly:assembly
docker build -t central-station:1.0.0 .
```