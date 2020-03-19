package ru.n1ppl3.spring.kafka;

import org.junit.jupiter.api.Test;


class MyKafkaConsumerTest {


	@Test
	void receiveTest() throws Exception {
		MyKafkaConsumer.receive("my-topic");
	}

}
