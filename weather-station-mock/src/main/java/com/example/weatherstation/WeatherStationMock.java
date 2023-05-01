package com.example.weatherstation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WeatherStationMock {

	public static final String TOPIC = "weather-station-status";

	public static void main(String[] args) {
		WeatherStatusProducer weatherStatusProducer = new WeatherStatusProducer();
		KafkaProducer<String, byte[]> producer = createProducer();

		while (true) {
			try {

				// drop 10% of the messages
				if (generateRandomNumberFromOneToTen() == 10) {
					continue;
				}

				Status status = weatherStatusProducer.getNextWeatherStatus();
				// create a producer record with key status.isNo and value as status
				producer.send(new ProducerRecord(TOPIC,
						status.getsNo().toString(), convertStatusToByteArray(status)));

				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
				producer.close();
				return;
			}
		}

	}

	static int generateRandomNumberFromOneToTen() {
		return (int) (Math.random() * 10) + 1;
	}

	static KafkaProducer<String, byte[]> createProducer() {
		return new KafkaProducer<>(createProduceProps());
	}

	static Properties createProduceProps() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // TODO: make an env variable
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		// props.put("acks", "all");
		// props.put("retries", 0);
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("buffer.memory", 33554432);

		return props;
	}

	static byte[] convertStatusToByteArray(Status status) throws IOException {
		// convertStatus to ByteArray
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(status);
		oos.flush();
		return bos.toByteArray();
	}

}