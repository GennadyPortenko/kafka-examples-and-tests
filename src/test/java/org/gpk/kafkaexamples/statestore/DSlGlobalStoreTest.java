package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
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
public class DSlGlobalStoreTest {
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();

        streamsBuilder.addGlobalStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("global-store-1"),
                        Serdes.String(),
                        Serdes.String())
                        .withLoggingDisabled(),
                "input",
                Consumed.with(Serdes.String(), Serdes.String()),
                GlobalStoreProcessor::new
        );

        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    class GlobalStoreProcessor implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore) context.getStateStore("global-store-1");

            log.info("records from store : ");
            kvStore.all().forEachRemaining(kv -> log.info("store record - key: {}, value : {}", kv.key, kv.value));
        }
        @Override
        public void close() { }
        @Override
        public void process(String key, String value) {
            // context.forward(key, value);
            log.info("process - key : {}, value : {}", key, value);
            log.info("records from store : ");
            kvStore.all().forEachRemaining(kv -> log.info("store record - key: {}, value : {}", kv.key, kv.value));
        }
    }


    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-global-store-example");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

}
