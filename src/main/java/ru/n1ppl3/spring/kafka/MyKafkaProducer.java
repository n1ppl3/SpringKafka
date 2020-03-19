package ru.n1ppl3.spring.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MyKafkaProducer {
	
	
	public static void send(String topicName) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		
		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		
		List<Future<RecordMetadata>> futures = new ArrayList<>();
		try {
			for (int i=0; i < 100; i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i));
				Future<RecordMetadata> result = producer.send(producerRecord);
				futures.add(result);
				logger.info("{}: {}", i, result);
			}
		} finally {
			producer.close();
		}
		
		futures.forEach(f -> {
			try {
				logger.info("{}", f.get());
			} catch (InterruptedException | ExecutionException e) {
				logger.error(e.getLocalizedMessage(), e);
			}
		});
	}

}
