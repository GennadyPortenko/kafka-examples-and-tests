package org.gpk.kafkaexamples.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class ProducerTest {

    @Test
    public void producerExampleTest() {
        final String topic = "test";
        try (KafkaProducer<String, String> producer = createProducer(topic)) {
            System.out.println("size : " + new ProducerRecord<>(topic, "1", "123456").value().getBytes().length);

            producer.send(new ProducerRecord<>(topic, "1", "123"));
            producer.send(new ProducerRecord<>(topic, "1", "1234"));
            producer.send(new ProducerRecord<>(topic, "1", "12345"));
            producer.send(new ProducerRecord<>(topic, "1", "123456"));
            producer.send(new ProducerRecord<>(topic, "1", "123"));
        }
    }

    public KafkaProducer<String, String> createProducer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 90);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        return producer;
    }
}
