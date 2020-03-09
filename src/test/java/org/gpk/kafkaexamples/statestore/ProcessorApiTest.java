package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
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
import java.util.UUID;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class ProcessorApiTest {

    @Test
    public void start() throws InterruptedException {

        Topology builder = new Topology();

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-example_" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        builder
                .addSource("Source", "input")                    //define source topic
                .addProcessor("Processor1", () -> new Processor1(), "Source")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("store1"), Serdes.String(), Serdes.String()), "Processor1");

                /*
                .addProcessor("Processor2", new ProcessorSupplier<String, String>() {            //include second custom processor
                    public Processor<String, String> get() {
                        return new Processor2();
                    }
                }, "Processor1")
                 */
        // .addSink("Sink", "Output-Topic", "Processor2")                        //link store Count to first processor
        // .connectProcessorAndStateStores("Processor2", "Count");              //define the output topic

        KafkaStreams streams = new <String, String>KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    class Processor1 implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;

        @Override
        public void close() {
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore) context.getStateStore("store1");
            kvStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
        }

        @Override
        public void process(String key, String value) {
            log.info("records from store:");
            kvStore.all().forEachRemaining(kv -> {
                log.info("store record - key : {}, value : {}", kv.key, kv.value);
            });
            log.info("k : {}, v : {}", key, value);
            kvStore.put(key, value);
            // context.forward(key, value);
        }

    }

}
