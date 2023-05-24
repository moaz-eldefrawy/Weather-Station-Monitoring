package com.example.centralstation;

public class Constants {
  // If you are running locally but you want to connect to the k8s kafka, set this to localhost:30000
  // If running in the k8s cluster, set this to kafka:9092
  public static final String BOOTSTRAP_SERVERS = "kafka:9092"; 
  public static final String ELASTIC_SEARCH_SERVER = "localhost:9200";
  public static final String GROUP_ID = "test-group";
  public static final String WEATHER_STATION_STATUS_TOPIC = "weather-station-status";
  public static final String HUMIDITY_NOTIFICATION_TOPIC = "humidity-notification";
}
