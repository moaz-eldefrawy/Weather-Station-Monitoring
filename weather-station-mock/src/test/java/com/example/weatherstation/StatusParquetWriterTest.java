package com.example.weatherstation;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatusParquetWriterTest {
  static final String PARQUET_FOLDER = "parquet-data-test";
  static final String PARQUET_SCHEMA = "parquet.schema";

  @Before
  public void before() {
    File parquetDir = new File(PARQUET_FOLDER);
    if (parquetDir.exists()) {
      for (File file : parquetDir.listFiles()) {
        file.delete();
      }
      parquetDir.delete();
    }
    parquetDir.mkdirs();
  }

  @Test
  public void testWritesParquetFilesForDifferentStations() throws IOException {
    File parquetDir = new File(PARQUET_FOLDER);
    File parquetSchema = new File(PARQUET_SCHEMA);
    StatusParquetWriter statusParquetWriter = new StatusParquetWriter(parquetDir, parquetSchema);
    WeatherStatusProducer weatherStatusProducer = new WeatherStatusProducer();

    weatherStatusProducer.getBatteryStatus();
    // generate 20000 records for station 1 to 10
    for (long i = 1; i <= 10; i++) {
      for (int j = 0; j <= 20000; j++) {
        statusParquetWriter.write(weatherStatusProducer.getNextWeatherStatus().toBuilder().stationId(i).build());
      }
    }

    // check that the 2 files are created for each station
    for (long i = 1; i <= 10; i++) {
      File[] files = parquetDir.listFiles();
      int count = 0;
      for (File file : files) {

        // and file name ends with .parquet
        if (file.getName().endsWith(".parquet") &&
            file.getName().split("\\.")[1].equals(Long.toString(i))) {
          count++;
        }
      }
      assertEquals(2, count);
    }
  }
}
