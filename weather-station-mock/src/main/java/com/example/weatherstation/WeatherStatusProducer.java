package com.example.weatherstation;

public class WeatherStatusProducer {
  private Long isNoCounter = 1L;

  WeatherStatusProducer() {
  }

  Status getNextWeatherStatus() {
    // create a dummy weather
    Weather weather = Weather.builder()
        .humidity(10)
        .temperature(20)
        .windSpeed(30)
        .build();

    Status status = Status.builder()
        .stationId(1L) // TODO get from env variable
        .sNo(isNoCounter++)
        .batteryStatus(getBatteryStatus())
        .statusTimestamp(System.currentTimeMillis())
        .weather(weather)
        .build();

    return status;
  }

  String getBatteryStatus() {
    // Randomly, change the battery_status field by the following specs:
    // ○ Low = 30% of messages per service
    // ○ Medium = 40% of messages per service
    // ○ High = 30% of messages per service

    int randomNumber = generateRandomNumberFromOneToTen();

    if (randomNumber <= 3) {
      return "LOW";
    } else if (randomNumber <= 7) {
      return "MEDIUM";
    } else {
      return "HIGH";
    }
  }

  static int generateRandomNumberFromOneToTen() {
    return (int) (Math.random() * 10) + 1;
  }

}
