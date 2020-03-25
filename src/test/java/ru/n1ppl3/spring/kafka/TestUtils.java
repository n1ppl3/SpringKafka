package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonMap;
import static ru.n1ppl3.utils.ReflectUtils.getObjectFieldValue;


@Slf4j
public abstract class TestUtils {


    public static <K, V> String getProducerClientId(KafkaProducer<K, V> kafkaProducer) {
        return (String) getObjectFieldValue(kafkaProducer, "clientId");
    }

    public static <K, V> String getConsumerClientId(KafkaConsumer<K, V> kafkaProducer) {
        return (String) getObjectFieldValue(kafkaProducer, "clientId");
    }


    public static void deleteTopics(Admin kafkaAdmin, String... topics) {
        DeleteTopicsResult deleteTopicsResult = kafkaAdmin.deleteTopics(Arrays.asList(topics));
        deleteTopicsResult.values().forEach((k, v) -> {
            try {
                logger.info("deleteTopics: {}: {}", k, v.get());
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    logger.warn("{}", e.getLocalizedMessage());
                } else {
                    throw new KafkaException(e);
                }
            }
        });
    }


    public static void ensureTopics(String bootstrapServers, NewTopic... topics) {
        try (Admin kafkaAdmin = createKafkaAdmin(bootstrapServers)) {
            ensureTopics(kafkaAdmin, topics);
        }
    }


    public static void ensureTopics(Admin kafkaAdmin, NewTopic... topics) {
        Collection<NewTopic> newTopics = Arrays.asList(topics);

        CreateTopicsOptions options = new CreateTopicsOptions();
        options.validateOnly(false);

        CreateTopicsResult createTopicsResult = kafkaAdmin.createTopics(newTopics, options);
        try {
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new KafkaException(e);
            }
        }
        logger.info("createTopicsResult: {}", createTopicsResult.values());

        try {
            logger.info("listTopics: {}", kafkaAdmin.listTopics().namesToListings().get());
            Collection<String> topicNames = kafkaAdmin.listTopics().names().get();
            Map<String, TopicDescription> description = kafkaAdmin.describeTopics(topicNames).all().get();
            description.forEach((k, v) -> logger.info("describeTopics: {}: {}", k, v));
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }


    public static Admin createKafkaAdmin(String bootstrapServers) {
        Map<String, Object> conf = singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(conf);
    }

}
