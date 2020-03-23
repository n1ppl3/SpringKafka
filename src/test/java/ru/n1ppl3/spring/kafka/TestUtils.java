package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@Slf4j
public abstract class TestUtils {


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

}
