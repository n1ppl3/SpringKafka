package ru.n1ppl3.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.n1ppl3.spring.kafka.PlainJavaSpringExample.consumerProps;
import static ru.n1ppl3.spring.kafka.PlainJavaSpringExample.senderProps;

@ExtendWith({SpringExtension.class})
@ContextConfiguration(classes = {JavaConfigurationExample.Config.class})
public class JavaConfigurationExample {

    @EnableKafka
    @Import({Listener.class})
    static class Config {

        // consumer

        @Bean
        ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            return factory;
        }

        @Bean
        public ConsumerFactory<Integer, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerProps());
        }

        // producer

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(senderProps());
        }

    }


    @Autowired
    private Listener listener;
    @Autowired
    private KafkaTemplate<Integer, String> template;


    @Test
    public void testSimple() throws Exception {
        Thread.sleep(1_000); // wait a bit for the container to start
        template.send("annotated1", 0, "foo");
        template.flush();
        assertTrue(listener.latch1.await(10, TimeUnit.SECONDS));
    }


    @Slf4j
    static class Listener {

        private final CountDownLatch latch1 = new CountDownLatch(1);

        @KafkaListener(id = "foo", topics = "annotated1")
        public void listen1(String foo) {
            logger.info("received {}", foo);
            latch1.countDown();
        }

    }

}
