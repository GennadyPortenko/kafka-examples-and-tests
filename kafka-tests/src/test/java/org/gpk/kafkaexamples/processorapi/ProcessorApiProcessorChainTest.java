package org.gpk.kafkaexamples.processorapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Properties;
import java.util.UUID;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class ProcessorApiProcessorChainTest {


    /*
      ProcessorContext::commit refers to concrete processor and not to processor chain
    */
    @Test
    public void start() throws InterruptedException {

        Topology builder = new Topology();

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put("enable.auto.commit", "false");

        builder
                .addSource("Source", "input")
                .addProcessor("Processor1", Processor1::new, "Source")
                .addSink("Sink1", "intermediate", "Processor1")
                .addSource("Source2", "intermediate")
                .addProcessor("Processor2", Processor2::new, "Source2")
                .addSink("Sink2", "output", "Processor2");

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
            log.info("processor1 - k : {}, v : {}", key, value);
            context.forward(key, value);
            context.commit();
        }
    }

    class Processor2 implements Processor<String, String> {
        private ProcessorContext context;
        @Override
        public void close() {}
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }
        @Override
        public void process(String key, String value) {
            log.info("processor2 - k : {}, v : {}", key, value);
            context.forward(key, value);
        }
    }

}
