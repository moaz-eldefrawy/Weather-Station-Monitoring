package com.example.centralstation.models;

import lombok.Builder;

@Builder(toBuilder = true)
public class Status implements java.io.Serializable {
  // add these fields
  private Long stationId;
  private Long sNo;
  private String batteryStatus;
  private Long statusTimestamp;
  private Weather weather;

  public Long getStationId() {
    return stationId;
  }

  public Long getsNo() {
    return sNo;
  }

  public String getBatteryStatus() {
    return batteryStatus;
  }

  public Long getStatusTimestamp() {
    return statusTimestamp;
  }

  public Weather getWeather() {
    return weather;
  }

  @Override
  public String toString() {
    return "Status{" +
        "stationId=" + stationId +
        ", sNo=" + sNo +
        ", batteryStatus='" + batteryStatus + '\'' +
        ", statusTimestamp=" + statusTimestamp +
        ", weather=" + weather.toString() +
        '}';
  }

}
