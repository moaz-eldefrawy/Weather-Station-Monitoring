# Weather-Station-Monitoring

[![](misc/demo.gif)](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--glare-effect)

## [Demo 💥]()

## System Design

<img src="./system-design.svg">

### Overview

- Each weather station outputs a status message every 1 second to report its weather status to Kafka.
- The Central Station consumes these weather status messages does the following:
  - Check for high humidity and notify if it's above a certain level
  - Batch archive data in parquet files
  - Write data to the bitcask lsm

- Then a connector fetches the parquet data into ES to be analyzed by kibana and report battry status like the following
  <img src="./kibana.png" width="300" height="200">

### LSM

We built our own LSM from scratch implementing the following:
- write files to segments and maintain their hint files
- compaction over the segments to reduce memory usage
- synchronizing between the compaction, reader and writer threads.
- fast recovery using hint files

## Development Installation

- install java 11
- install maven
- run `mvn install `
- install docker
- run `docker compose up -d`
- run `WeatherStationMock.java` and `CentralStation.java`
- verfiy that everything is working by quering kibana by running `GET weather-status/_search` in the dev tools editor or query elastic search directly
