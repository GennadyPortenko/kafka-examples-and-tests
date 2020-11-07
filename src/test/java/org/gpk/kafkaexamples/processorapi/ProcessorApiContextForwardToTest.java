package org.gpk.kafkaexamples.processorapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
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
public class ProcessorApiContextForwardToTest {

    @Test
    public void start() throws InterruptedException {

        Topology builder = new Topology();

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        builder
                .addSource("Source", "input")
                .addProcessor("Processor1", () -> new Processor1(), "Source")
                .addSink("Sink1", "output", "Processor1");

        KafkaStreams streams = new <String, String>KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    class Processor1 implements Processor<String, String> {
        private ProcessorContext context;
        @Override
        public void close() {}
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }
        @Override
        public void process(String key, String value) {
            context.forward(key, value, To.child("Sink1"));
        }

    }

}
