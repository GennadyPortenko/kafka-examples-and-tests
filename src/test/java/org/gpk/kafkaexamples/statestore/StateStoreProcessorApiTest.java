package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Properties;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class StateStoreProcessorApiTest {

    @Test
    public void start() throws InterruptedException {

        Topology builder = new Topology();

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "statestore-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        builder
                .addSource("source", "input")                    //define source topic
                .addProcessor("processor1", Processor1::new, "source")
                .addGlobalStore(
                        Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("globalStore1"),
                            Serdes.String(), Serdes.String())
                            .withLoggingDisabled(), // logging must be disabled for global stores
                       "globalStoreSource",
                            new StringDeserializer(),
                            new StringDeserializer(),
                       "globalStoreTopic",
                       "GlobalStoreProcessor1", GlobalStoreProcessor1::new
                        )
                .addSink("sink", "globalStoreTopic", "processor1")
                .addSink("fromGlobalStore", "fromGlobalStoreTopic", "GlobalStoreProcessor1");

        KafkaStreams streams = new <String, String>KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    static class GlobalStoreProcessor1 implements Processor<String, String> {
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }
        @Override
        public void close() { }
        @Override
        public void process(String key, String value) {
            log.info("global store - new message - key : {}, value : {}", key, value);

            /*
               context.forward(key, value);
               will throw runtime exception on new message produced to global store topic,
               global store processor can not call forward() if it has child nodes
            */
        }
    }


    static class Processor1 implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;

        @Override
        public void close() { }
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore) context.getStateStore("globalStore1");
            kvStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
        }
        @Override
        public void process(String key, String value) {
            log.info("k : {}, v : {}", key, value);
            log.info("records from global store:");
            kvStore = (KeyValueStore) context.getStateStore("globalStore1");
            kvStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
            context.forward(key, value);
            context.commit();
        }

    }

}
