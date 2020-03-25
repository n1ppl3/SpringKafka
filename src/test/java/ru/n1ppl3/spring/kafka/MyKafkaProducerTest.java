package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static ru.n1ppl3.spring.kafka.TestUtils.ensureTopics;


@Slf4j
class MyKafkaProducerTest {

	@BeforeAll
	static void beforeAll() {
		ensureTopics("localhost:9092", new NewTopic("my-topic", 1, (short) 1));
	}

	@Test
	void syncSendTest() {
		MyKafkaProducer.syncSend("my-topic");
	}

	@Test
	void asyncSyncSendTest() {
		MyKafkaProducer.asyncSyncSend("my-topic");
	}

	@Test
	void asyncSendTest() {
		MyKafkaProducer.asyncSend("my-topic");
	}

}
