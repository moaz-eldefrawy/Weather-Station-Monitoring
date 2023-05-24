package com.example.centralstation.models;

import lombok.Builder;

@Builder(toBuilder = true)

public class Weather implements java.io.Serializable {
  private Integer humidity;
  private Integer temperature;
  private Integer windSpeed;

  public Integer getHumidity() {
    return humidity;
  } 

  public Integer getTemperature() {
    return temperature;
  }

  public Integer getWindSpeed() {
    return windSpeed;
  }

  @Override
  public String toString() {
    return "Weather{" +
        "humidity=" + humidity +
        ", temperature=" + temperature +
        ", windSpeed=" + windSpeed +
        '}';
  }
}
