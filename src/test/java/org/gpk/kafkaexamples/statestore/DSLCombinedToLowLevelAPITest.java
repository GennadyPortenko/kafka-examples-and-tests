package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Transformer;
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
public class DSLCombinedToLowLevelAPITest {
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-lowlevel-example" + UUID.randomUUID());

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("store1"),
                Serdes.String(),
                Serdes.String());
        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder.<String, String>stream("input")
                .peek((k, v) -> log.info("key : {}, value : {}", k, v))
                .transform(Transformer1::new, "store1")
                .foreach((k, v) -> {});

        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    static class Transformer1 implements Transformer<String, String, KeyValue<String, String>> {
        private ProcessorContext context;
        private KeyValueStore<String, String> stateStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.stateStore = (KeyValueStore<String, String>) context.getStateStore("store1");
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {

            log.info("records from store:");
            stateStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
            log.info("k : {}, v : {}", key, value);
            stateStore.put(key, value);

            // transform key and/or value or use context.forward to produce multiple messages
            return new KeyValue<>(key, value);
        }

        @Override
        public void close() {  }
    }


    @Test
    public void flatmapTransformerTest() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("store1"),
                Serdes.String(),
                Serdes.String());
        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder.<String, String>stream("input")
                .peek((k, v) -> log.info("key : {}, value : {}", k, v))
                .transform(FlatmapTransformer::new, "store1")
                .foreach((k, v) -> log.info("output k : {}, v : {}", k, v));

        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    static class FlatmapTransformer implements Transformer<String, String, KeyValue<String, List<String>>> {
        private ProcessorContext context;
        private KeyValueStore<String, String> stateStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.stateStore = (KeyValueStore<String, String>) context.getStateStore("store1");
        }

        @Override
        public KeyValue<String, List<String>> transform(String key, String value) {

            log.info("records from store:");
            stateStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
            log.info("store process - k : {}, v : {}", key, value);

            context.forward(key, Arrays.asList("1", "2", "3"));
            if (true) {
                throw new RuntimeException("exception between output records");
            }
            context.forward(key, Arrays.asList("4", "5", "6"));
            return null;
        }

        @Override
        public void close() {  }
    }

    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-lowlevel-example");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

}
