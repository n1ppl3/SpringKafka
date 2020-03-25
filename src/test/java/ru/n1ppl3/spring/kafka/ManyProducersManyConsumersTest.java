package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static ru.n1ppl3.spring.kafka.ExecUtils.executeParallelTasks;

/**
 * если консумеров в одной группе больше, чем партиций, то они все работают, но "лишние" получают 0 записей
 * TODO, сделать тесты с консумерами в разных группах
 */
@Slf4j
class ManyProducersManyConsumersTest {

    private static final String KAFKA_BROKER_ADDR = "localhost:9092";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");


    @Test
    void test_1Partition_1Consumer() throws Exception {
        String topicName = generateTopicName();
        startProducers(1, topicName);
        List<Integer> consumeResults = startConsumers(1, topicName);
        Assertions.assertEquals(singletonList(10), consumeResults);
    }


    @Test
    void test_1Partition_2Consumers() throws Exception {
        String topicName = generateTopicName();
        startProducers(1, topicName);
        List<Integer> consumeResults = startConsumers(2, topicName);
        Assertions.assertEquals(new TreeSet<>(asList(0, 10)), new TreeSet<>(consumeResults));
    }


    @Test
    void test_2Partitions_1Consumer() throws Exception {
        String topicName = generateTopicName();
        TestUtils.ensureTopics(KAFKA_BROKER_ADDR, new NewTopic(topicName, 2, (short) 1));
        startProducers(1, topicName);
        List<Integer> consumeResults = startConsumers(1, topicName);
        Assertions.assertEquals(singletonList(10), consumeResults);
    }


    @Test
    void test_2Partitions_2Consumers() throws Exception {
        String topicName = generateTopicName();
        TestUtils.ensureTopics(KAFKA_BROKER_ADDR, new NewTopic(topicName, 2, (short) 1));
        startProducers(1, topicName);
        List<Integer> consumeResults = startConsumers(2, topicName);
        Assertions.assertEquals(new TreeSet<>(asList(4, 6)), new TreeSet<>(consumeResults));
    }


    // producers

    private static List<Integer> startProducers(int count, String topicName) throws InterruptedException, ExecutionException {
        List<Callable<Integer>> callableList = new ArrayList<>(count);
        for (int i=0; i < count; i++) {
            callableList.add(() -> sendMessages(createProducer(), topicName));
        }
        return executeParallelTasks(callableList);
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_ADDR);
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private static Integer sendMessages(KafkaProducer<String, String> producer, String topicName) throws ExecutionException, InterruptedException {
        String producerName = TestUtils.getProducerClientId(producer);
        int messagesCount = 10;
        for (int i=0; i < messagesCount; i++) {
            String key = Integer.toString(i);
            String value = key + " from " + producerName;
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topicName, key, value));
            logger.info("{} sent message #{} and received {} from broker", producerName, i, result.get());
            Thread.sleep(500);
        }
        producer.close();
        return messagesCount;
    }


    // consumers

    private static List<Integer> startConsumers(int count, String topicName) throws InterruptedException, ExecutionException {
        List<Callable<Integer>> callableList = new ArrayList<>(count);
        for (int i=0; i < count; i++) {
            callableList.add(() -> consumeMessages(createConsumer(), topicName));
        }
        return executeParallelTasks(callableList);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_ADDR);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // иначе он не видит только что посланные сообщения
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private static Integer consumeMessages(KafkaConsumer<String, String> consumer, String topicName) throws InterruptedException {
        consumer.subscribe(Collections.singleton(topicName));

        String consumerName = TestUtils.getConsumerClientId(consumer);

        logger.info("{} GONNA POLL", consumerName);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
        logger.info("{} SUCCESSFULLY POLLED {} RECORDS", consumerName, records.count());

        for (ConsumerRecord<String, String> record : records) {
            logger.info("{}:\ntopic={}, partition={}, offset={}, key={}, value={}\n", consumerName,
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
            Thread.sleep(1000);
        }

        consumer.close();

        return records.count();
    }


    private static String getGroupId() {
        return "my-group-" + LocalDateTime.now().withNano(0).format(FORMATTER);
    }

    private static String generateTopicName() {
        return "myTopic" + LocalDateTime.now().withNano(0).format(FORMATTER);
    }

}
