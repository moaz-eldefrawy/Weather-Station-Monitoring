#
# BUILD STAGE
#
FROM maven:3.6.0-jdk-11-slim AS build  
COPY src /usr/src/app/src  
COPY pom.xml /usr/src/app  
RUN mvn -f /usr/src/app/pom.xml dependency:resolve 
RUN mvn -f /usr/src/app/pom.xml assembly:assembly

#
# PACKAGE STAGE
#
FROM openjdk:11-jre-slim 
COPY --from=build /usr/src/app/target/weather-station.jar /usr/app/weather-station.jar 
EXPOSE 8080  
CMD ["java","-jar","/usr/app/weather-station.jar"]  