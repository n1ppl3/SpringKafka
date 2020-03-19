package ru.n1ppl3.spring.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MyKafkaConsumer {


	public static void receive(String... topics) throws InterruptedException {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "test");
		// Setting enable.auto.commit means that offsets are committed automatically with a frequency controlled by the config auto.commit.interval.ms.
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "600");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
		try {
			consumerWork(consumer, topics);
		} finally {
			consumer.close();
		}
	}


	private static void consumerWork(KafkaConsumer<String, String> consumer, String... topics) throws InterruptedException {
		consumer.subscribe(Arrays.asList(topics), new MyListener());
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
			}
			
			Set<TopicPartition> assigned = consumer.assignment();
			logger.info("ASSIGNED: {}", assigned);
			
			logger.info("BEGIN:    {}", consumer.beginningOffsets(assigned));
			logger.info("END:      {}", consumer.endOffsets(assigned));
			logger.info("COMMITED: {}", consumer.committed(assigned));
		}
	}


	@Slf4j
	static class MyListener implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			logger.info("LISTENER_REVOKED:  {}", partitions);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			logger.info("LISTENER_ASSIGNED: {}", partitions);
		}

	}

}
