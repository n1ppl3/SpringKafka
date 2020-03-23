package ru.n1ppl3.spring.kafka;

import java.io.IOException;

import org.junit.jupiter.api.Test;


class MyKafkaProducerTest {

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
