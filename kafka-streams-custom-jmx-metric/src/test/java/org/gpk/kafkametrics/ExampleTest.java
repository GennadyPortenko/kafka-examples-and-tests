package org.gpk.kafkametrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@Disabled
public class ExampleTest {
    private volatile int state = 0;

    @Test
    public void start() throws InterruptedException {

        Topology builder = new Topology();

        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        builder
                .addSource("INPUT-source", "input")
                .addProcessor("MAIN-processor", MainProcessor::new, "INPUT-source")
                .addSink("OUTPUT-sink", "output", "MAIN-processor");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-metrics-worker1");
        KafkaStreams streams1 = new <String, String>KafkaStreams(builder, props);
        streams1.setStateListener((newState, oldState) -> {
            state = newState.ordinal();
        });

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-metrics-worker2");
        KafkaStreams streams2 = new <String, String>KafkaStreams(builder, props);
        streams2.setStateListener((newState, oldState) -> {
            state = newState.ordinal();
        });

        streams1.start();
        streams2.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    private void addMetrics(ProcessorContext context, String workerId) {
        // domain name - kafka.streams
        // mbean name will be `kafka.streams:type=<TYPE>,<MBEAN_NAME>=<MBEAN_VALUE>`
        final String TYPE = "workers-metrics";
        final String MBEAN_KEY = "name";
        final String MBEAN_VALUE = "worker-metrics-" + workerId;
        final String STATE_ATTRIBUTE_NAME = "state";
        final String STATE_ATTRIBUTE_DESC = "Worker state";
        final String DUMMY_ATTRIBUTE_NAME = "dummy";
        final String DUMMY_ATTRIBUTE_DESC = "Dummy attribute, always 0";
        final String SENSOR_NAME = MBEAN_VALUE;

        StreamsMetrics streamsMetrics = context.metrics();

        Map<String, String> metricTags = new HashMap<>();
        metricTags.put(MBEAN_KEY, MBEAN_VALUE);

        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig);
        Sensor sensor = streamsMetrics.addSensor(SENSOR_NAME, Sensor.RecordingLevel.INFO);

        MetricName stateAttributeName = metrics.metricName(STATE_ATTRIBUTE_NAME, TYPE, STATE_ATTRIBUTE_DESC);
        sensor.add(stateAttributeName, new MeasurableStat() {
            @Override
            public double measure(MetricConfig config, long now) {
                return state;
            }

            @Override
            public void record(MetricConfig config, double value, long timeMs) {}
        });

        MetricName dummyAttributeName = metrics.metricName(DUMMY_ATTRIBUTE_NAME, TYPE, DUMMY_ATTRIBUTE_DESC);
        sensor.add(dummyAttributeName, new MeasurableStat() {
            @Override
            public double measure(MetricConfig config, long now) {
                return 0;
            }

            @Override
            public void record(MetricConfig config, double value, long timeMs) {}
        });
    }

    class MainProcessor implements Processor<String, String> {
        private ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;

            addMetrics(context, UUID.randomUUID().toString());
        }

        @Override
        public void process(String key, String value) {
            context.forward(key, value, To.child("OUTPUT-sink"));
        }

        @Override
        public void close() {}
    }

}
