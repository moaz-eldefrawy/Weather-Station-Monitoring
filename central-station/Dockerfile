#
# BUILD STAGE
#
FROM maven:3.6.0-jdk-11-slim AS build 
ENV HOME=/usr/app
RUN mkdir -p $HOME 
WORKDIR $HOME
ADD pom.xml $HOME
RUN mvn -f ${HOME}/pom.xml dependency:resolve 
COPY src ${HOME}/src  
COPY parquet.schema ${HOME}/
RUN mvn -f ${HOME}/pom.xml assembly:assembly

#
# PACKAGE STAGE
#
FROM openjdk:11-jre-slim 
COPY --from=build /usr/app/target/central-station.jar /usr/app/central-station.jar 
COPY --from=build /usr/app/parquet.schema /parquet.schema 
EXPOSE 8080
CMD ["java","-jar","/usr/app/central-station.jar"]  