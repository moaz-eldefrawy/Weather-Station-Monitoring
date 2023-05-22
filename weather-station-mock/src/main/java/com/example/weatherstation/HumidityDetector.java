package com.example.weatherstation;

import java.io.ByteArrayInputStream;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.weatherstation.models.Status;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class HumidityDetector {
  final static String GROUP_ID = "test-group";
  final static int HUMIDITY_THRESHOLD = 70;

  public static void main(String[] args) {
    detect();
  }

  public static void detect() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
    KafkaProducer<String, String> specialMessageProducer = new KafkaProducer<>(createProduceProps());
    consumer.subscribe(Collections.singleton(Constants.WEATHER_STATION_STATUS_TOPIC));

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, byte[]> record : records) {
        try {
          Status status = HumidityDetector.convertByteArrayToStatus(record.value());
          System.out.println(status.toString());
          if (status.getWeather().getHumidity() > HUMIDITY_THRESHOLD) {
            // send message to topic
            specialMessageProducer.send(new ProducerRecord<String, String>(Constants.HUMIDITY_NOTIFICATION_TOPIC, "key",
                "Some Special Message of " + status.getWeather().getHumidity().toString()));
          }
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }

      }
    }
  }

  static Status convertByteArrayToStatus(byte[] array) throws IOException, ClassNotFoundException {
    // convertStatus to ByteArray
    ByteArrayInputStream bis = new ByteArrayInputStream(array);
    ObjectInputStream ois = new ObjectInputStream(bis);
    return (Status) ois.readObject();
  }

  static Properties createProduceProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS); // TODO: make an env variable
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", StringSerializer.class.getName());

    // props.put("batch.size", 16384);
    // props.put("linger.ms", 1);
    // props.put("buffer.memory", 33554432);

    return props;
  }

}
