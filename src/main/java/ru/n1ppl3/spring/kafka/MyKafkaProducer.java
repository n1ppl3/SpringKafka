package ru.n1ppl3.spring.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MyKafkaProducer {

	private static final int REPEATS = 100;


	private static Producer<String, String> myProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// props.put(ProducerConfig.ACKS_CONFIG, "all");

		return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
	}


	public static void syncSend(String topicName) {
		try (Producer<String, String> producer = myProducer()) {
			for (int i=0; i < REPEATS; i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i));
				Future<RecordMetadata> result = producer.send(producerRecord);
				safeGet(result);
			}
		}
	}

	
	public static void asyncSyncSend(String topicName) {
		Producer<String, String> producer = myProducer();
		
		List<Future<RecordMetadata>> futures = new ArrayList<>(REPEATS);
		try {
			for (int i=0; i < REPEATS; i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i));
				Future<RecordMetadata> result = producer.send(producerRecord);
				futures.add(result);
			}
		} finally {
			producer.close();
		}
		
		futures.forEach(MyKafkaProducer::safeGet);
	}


	public static void asyncSend(String topicName) {
		Callback producerCallback = (metadata, exception) -> {
			if (exception != null) {
				logger.error(exception.getLocalizedMessage(), exception);
			} else {
				logger.info("metadata: {}", metadata);
			}
		};

		try (Producer<String, String> producer = myProducer()) {
			for (int i=0; i < REPEATS; i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i));
				Future<RecordMetadata> result = producer.send(producerRecord, producerCallback);
				safeGet(result);
			}
		}
	}


	private static <T> void safeGet(Future<T> future) {
		try {
			logger.info("future: {}", future.get());
		} catch (InterruptedException | ExecutionException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

}
