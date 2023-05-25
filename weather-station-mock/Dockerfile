#
# PACKAGE STAGE
#
FROM openjdk:11-jre-slim 
COPY ./target/weather-station.jar /usr/app/weather-station.jar 
CMD ["sh", "-c", "java -jar /usr/app/weather-station.jar $(hostname | tail -c 2)"]  
