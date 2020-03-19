package ru.n1ppl3.spring.kafka;

import java.io.IOException;

import org.junit.jupiter.api.Test;


class MyKafkaProducerTest {


	@Test
	void sendTest() throws IOException {
		MyKafkaProducer.send("my-topic");
	}

}
