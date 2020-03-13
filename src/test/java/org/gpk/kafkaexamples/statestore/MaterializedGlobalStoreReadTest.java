package org.gpk.kafkaexamples.statestore;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
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
public class MaterializedGlobalStoreReadTest {

    /**
     * (throws a runtime exception)
     * we can have only one source of global store topic in one kafka streams
     */
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-global-store-read-test");

        final String globalStoreTopic = "materialized-global-store-log";

        KeyValueBytesStoreSupplier funcStoreSupplier = Stores.inMemoryKeyValueStore("MaterializedGlobalStore");
        streamsBuilder.globalTable(globalStoreTopic, Materialized.as(funcStoreSupplier));

        streamsBuilder.<String, String>stream(globalStoreTopic)
                .foreach((k, v) -> log.info("global store record - key : {}, value : {}", k, v));

        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
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
