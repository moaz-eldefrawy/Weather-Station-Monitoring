package com.example.centralstation;

import com.example.weatherstation.models.Status;
import com.example.weatherstation.models.Weather;

public class WeatherStatusProducer {
  private Long isNoCounter = 1L;

  WeatherStatusProducer() {
  }

  public Status getNextWeatherStatus() {
    // create a dummy weather
    Weather weather = Weather.builder()
        .humidity(generateRandomNumberFromRange(0, 100))
        .temperature(generateRandomNumberFromRange(20, 30))
        .windSpeed(generateRandomNumberFromRange(10, 15))
        .build();

    // TODO(woofy): get station id from env variable
    return Status.builder()
        .stationId(1L)
        .sNo(isNoCounter++)
        .batteryStatus(getBatteryStatus())
        .statusTimestamp(System.currentTimeMillis())
        .weather(weather)
        .build();
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
