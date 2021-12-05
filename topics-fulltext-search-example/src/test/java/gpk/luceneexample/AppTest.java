package gpk.luceneexample;

import gpk.luceneexample.data.IndexRecord;
import gpk.luceneexample.search.Searcher;
import gpk.luceneexample.write.Writer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.io.IOException;
import java.util.Properties;

import static gpk.luceneexample.data.IndexRecord.RecordField.*;

/**
 * queries:
 * kafkacat -b localhost:9095 -L
 * kafkacat -b localhost:9095 -P -t input -K:
 * kafkacat -b localhost:9095 -P -t query
 *
 * sample output:
 * received -  k : 1  v : hello
 * received -  k : 2  v : hello, rabbit
 * query : hello
 * Found 2 hit(s).
 * 1. 1 -> hello, 1638715050718
 * 2. 2 -> hello, rabbit, 1638715073197
 * query : rabbit
 * Found 1 hit(s).
 * 1. 2 -> hello, rabbit, 1638715073197
 */
public class AppTest {
    @BeforeAll
    static void runEmbeddedKafka() throws InterruptedException {
        new KafkaCluster(9095, 12010);
    }

    @Test
    void runApp() throws InterruptedException {
        Topology builder = new Topology();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = new ByteBuffersDirectory();

        builder
                .addSource("InputSource", "input")
                .addProcessor("InputProcessor", () -> new Processor<String, String, String, String>() {
                    private ProcessorContext<String, String> context;
                    @Override
                    public void close() {}

                    @Override
                    public void init(ProcessorContext<String, String> context) {
                        this.context = context;
                        System.out.println("input processor initialized");
                    }

                    @Override
                    public void process(Record<String, String> record) {
                        System.out.println("received -  k : " + record.key() + "  v : " + record.value());

                        try (Writer writer = new Writer(analyzer, indexDirectory)) {
                            String key = record.key();
                            String value = record.value();
                            long timestamp = record.timestamp();

                            IndexRecord indexRecord = new IndexRecord(record.key(), record.value(), record.timestamp());

                            writer.addDocument(indexRecord.toDocument());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, "InputSource")
                .addSource("QuerySource", "query")
                .addProcessor("QueryProcessor", () -> new Processor<String, String, String, String>() {
                    private ProcessorContext<String, String> context;
                    @Override
                    public void close() {}

                    @Override
                    public void init(ProcessorContext<String, String> context) {
                        this.context = context;
                        System.out.println("query processor initialized");
                    }

                    @Override
                    public void process(Record<String, String> record) {
                        try (Searcher searcher = new Searcher(indexDirectory, analyzer)) {
                            String querystr = record.value();
                            System.out.println("query : " + querystr);
                            ScoreDoc[] hits = searcher.search(Value.name(), querystr);

                            // display results
                            System.out.println("Found " + hits.length + " hit(s).");
                            for (int i = 0; i < hits.length; ++i) {
                                Document document = searcher.getByDocumentId(hits[i].doc).orElse(null);
                                if (null != document) {
                                    System.out.println((i + 1) + ". " + document.get(Key.name()) + " -> " +
                                            document.get(Value.name()) + ", " +
                                            document.get(Timestamp.name()));
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, "QuerySource");

        KafkaStreams streams = new <String, String>KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    public static class KafkaCluster implements AutoCloseable {
        EmbeddedKafkaBroker embeddedKafkaBroker;

        public KafkaCluster(int kafkaPort, int zookeeperPort) {
            EmbeddedKafkaBroker kafka = new EmbeddedKafkaBroker(1, false)
                    .kafkaPorts(kafkaPort)
                    .zkPort(zookeeperPort);
            kafka.afterPropertiesSet(); // start
            kafka.addTopics(new NewTopic("input", 1, (short)1),
                    new NewTopic("query", 1, (short)1));
        }

        @Override
        public void close() {
            embeddedKafkaBroker.destroy();
        }
    }
}
