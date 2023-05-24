package com.example.centralstation;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.weatherstation.models.Status;

public class CentralStation {
  static final int HUMIDITY_THRESHOLD = 70;
  static final String LSM_FOLDER = "lsm-data";
  static final String PARQUET_FOLDER = "parquet-data";
  static final String PARQUET_SCHEMA = "parquet.schema";

  public static void main(String[] args) {
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(createConsumerProps());
    KafkaProducer<String, String> specialMessageProducer = new KafkaProducer<>(createProduceProps());
    File lsmDir = new File(LSM_FOLDER);
    File parquetDir = new File(PARQUET_FOLDER);
    File parquetSchema = new File(PARQUET_SCHEMA);
    lsmDir.mkdirs();
    parquetDir.mkdirs();
    StatusParquetWriter statusParquetWriter;
    try {
      statusParquetWriter = new StatusParquetWriter(parquetDir, parquetSchema);
    } catch (IOException e) {
      // rethrow e with message that we couldn't read the schema file
      throw new RuntimeException("Couldn't read schema file", e);
    }

    consumer
        .subscribe(Collections.singletonList(Constants.WEATHER_STATION_STATUS_TOPIC));

    try {
      while (true) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, byte[]> record : records) {
          Status status = convertByteArrayToStatus(record.value());
          System.out.println("Consumed something: " + status.toString());

          // TODO: make a separate class for this
          if (status.getWeather().getHumidity() > HUMIDITY_THRESHOLD) {
            specialMessageProducer.send(new ProducerRecord<String, String>(Constants.HUMIDITY_NOTIFICATION_TOPIC, "key",
                "Some Special Message of " + status.getWeather().getHumidity().toString()));
          }

          statusParquetWriter.write(status);

        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }

  static Properties createConsumerProps() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-consumer-group"); // Specify a consumer group ID
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    return props;
  }

  static Status convertByteArrayToStatus(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bis);
    return (Status) ois.readObject();
  }

  static Properties createProduceProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS); // TODO: make an env variable
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", StringSerializer.class.getName());

    return props;
  }
}
