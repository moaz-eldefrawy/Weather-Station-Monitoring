package com.example.weatherstation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class WeatherStatusProducer {
  private Long isNoCounter = 1L;

  WeatherStatusProducer() {
  }

  Status getNextWeatherStatus() {
    // create a dummy weather
    Weather weather = Weather.builder()
        .humidity(generateRandomNumberFromRange(30, 40))
        .temperature(generateRandomNumberFromRange(20, 30))
        .windSpeed(generateRandomNumberFromRange(10, 15))
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

  static int generateRandomNumberFromRange(int min, int max) {
    return (int) (Math.random() * (max - min)) + min;
  }
}
