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
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class DSlGlobalStoreTest2 {
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();

        streamsBuilder.addGlobalStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("global-store"),
                        Serdes.String(),
                        Serdes.String())
                        .withLoggingDisabled(),
                "global-store-log",
                Consumed.with(Serdes.String(), Serdes.String()),
                GlobalStoreProcessor::new
        );

        streamsBuilder.<String, String>stream("input")
                /*
                .<String, String>flatMap((k, v) -> Arrays.asList(new KeyValue<>(null, "2"),
                        new KeyValue<>(null, "1"),
                        new KeyValue<>(null, "0")))
                */
                .transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
                    ProcessorContext context;
                    private KeyValueStore<String, String> kvStore;
                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.kvStore = (KeyValueStore) context.getStateStore("global-store");

                        log.info("records from store : ");
                        kvStore.all().forEachRemaining(kv -> log.info("store record - key: {}, value : {}", kv.key, kv.value));
                    }
                    @Override
                    public KeyValue<String, String> transform(String key, String value) {
                        log.info("k : {}, v : {}", key, value);
                        // context.forward(key, value, To.child());

                        log.info("records from store : ");
                        kvStore.all().forEachRemaining(kv -> log.info("store record - key: {}, value : {}", kv.key, kv.value));

                        context.forward(key, value);
                        context.forward(key, value);
                        context.commit();
                        return null;
                    }
                    @Override
                    public void close() {}
                })
                .to("output");



        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    class GlobalStoreProcessor implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }
        @Override
        public void close() { }
        @Override
        public void process(String key, String value) {
            log.info("global store - new record - key : {}, value : {}", key, value);
        }
    }


    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-global-store-example");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("enable.auto.commit", false);
        props.put("auto.commit.interval", 0);

        return props;
    }

}
