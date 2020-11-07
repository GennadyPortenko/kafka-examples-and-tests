package org.gpk.kafkaexamples.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class ConsumerTest {

    @Test
    public void consumerExampleTest() {
        try (Consumer<String, String> consumer = createConsumer("test")) {
            while (true) {
                final ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.ofMillis(1000));

                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                });

                consumer.commitAsync();
            }
        }

        /*
        final int giveUp = 100;   int noRecordsCount = 0;


        consumer.close();
        System.out.println("DONE");

         */
    }

    public Consumer<String, String> createConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-java-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
