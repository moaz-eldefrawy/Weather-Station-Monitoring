#
# PACKAGE STAGE
#
FROM openjdk:11-jre-slim 
COPY ./target/central-station.jar /usr/app/central-station.jar 
COPY ./parquet.schema /parquet.schema 
EXPOSE 8080
CMD ["java","-jar","/usr/app/central-station.jar"]  