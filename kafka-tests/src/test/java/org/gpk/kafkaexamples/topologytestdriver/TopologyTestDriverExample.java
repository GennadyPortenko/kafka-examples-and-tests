package org.gpk.kafkaexamples.topologytestdriver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;

import java.util.Properties;

@Slf4j
public class TopologyTestDriverExample {

    @Test
    public void topologyTestDriverExample() {
        final String INPUT_TOPIC = "input-topic";
        final String OUTPUT_TOPIC = "output-topic";

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC)
                .filter((k, v) -> v != null)
                .to(OUTPUT_TOPIC);
        Topology topology = builder.build();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>("input-topic", new StringSerializer(), new StringSerializer());
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", "42"));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer());

        log.info("output record : {}", outputRecord);
    }
}
