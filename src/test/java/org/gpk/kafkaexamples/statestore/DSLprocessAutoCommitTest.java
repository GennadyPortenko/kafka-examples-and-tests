package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class DSLprocessAutoCommitTest {
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();

        streamsBuilder.<String, String>stream("input")
                .peek((k, v) -> log.info("key : {}, value : {}", k, v))
                .process(() -> new Processor<>() {
                    private ProcessorContext context;
                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }
                    @Override
                    public void process(String key, String value) {
                        // context.commit();
                    }
                    @Override
                    public void close() {}
                });

        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-autocommit-example");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        /*
          disable auto-commit
        */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);  // consumer auto commit interval
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE); // processor position auto save interval

        return props;
    }

}
