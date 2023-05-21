package com.example.weatherstation;

import java.io.IOException;

import org.junit.Test;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;

public class ElasticSeachIntegration {

  @Test
  public void test() throws ElasticsearchException, IOException {
    ElasticSearchStatusWriter elasticSearchStatusWriter = new ElasticSearchStatusWriter();
    WeatherStatusProducer weatherStatusProducer = new WeatherStatusProducer();
    for (int i = 0; i < 1; i++) {
      elasticSearchStatusWriter.write(weatherStatusProducer.getNextWeatherStatus());
    }
  }
}
