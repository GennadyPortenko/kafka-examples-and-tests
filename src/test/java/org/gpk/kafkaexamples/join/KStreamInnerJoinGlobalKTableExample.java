package org.gpk.kafkaexamples.join;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
public class KStreamInnerJoinGlobalKTableExample {
    @Test
    public void start() throws InterruptedException {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Properties props = props();

        KTable<String, String> ktable = streamsBuilder.table("input");

        GlobalKTable<String, String> globalKTable = streamsBuilder.globalTable("table-input");

        KStream<String, String>  stream = streamsBuilder.stream("global-table-input");

        stream.leftJoin(ktable, (v1, v2) -> v1 + " " + v2)
                .foreach((k, v) -> log.info("left join to KTable - k : {}, v : {}", k, v));

        stream.leftJoin(globalKTable, (k, v) -> k, (v1, v2) -> v1 + " " + v2)
                .foreach((k, v) -> log.info("left join to GlobalKTable - k : {}, v : {}", k, v));



        new KafkaStreams(streamsBuilder.build(), props).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-inner-join-ktable-example");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

}
