package org.gpk.kafkaexamples.ktable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class KTableTest {

    /* return the latest message within a time window */
    @Test
    public void windowedKGroupedStream() throws InterruptedException {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Properties props = properties();

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);

        streamsBuilder.<String, String>table("input")
                .toStream()
                .foreach((k, v) -> log.info("key : {}, value : {}", k, v));

        new KafkaStreams(streamsBuilder.build(), props).start();
        Thread.sleep(Long.MAX_VALUE);
    }


    Properties properties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return props;
    }
}
