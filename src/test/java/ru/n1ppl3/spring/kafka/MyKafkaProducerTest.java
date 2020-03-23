package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static ru.n1ppl3.spring.kafka.TestUtils.ensureTopics;


@Slf4j
class MyKafkaProducerTest {

	private static Admin kafkaAdmin = null;

	@BeforeAll
	static void beforeAll() {
		Map<String, Object> conf = singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaAdmin = Admin.create(conf);
		ensureTopics(kafkaAdmin, new NewTopic("my-topic", 1, (short) 1));
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


	@AfterAll
	static void afterAll() {
		kafkaAdmin.close();
	}

}
