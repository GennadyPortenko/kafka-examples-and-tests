package gpk.luceneexample;

import gpk.luceneexample.data.IndexRecord;
import gpk.luceneexample.search.Searcher;
import gpk.luceneexample.write.Writer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static gpk.luceneexample.data.IndexRecord.RecordField.*;

public class LuceneTest {
    @Disabled
    @Test
    void testLuceneOps() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = new ByteBuffersDirectory();

        List<IndexRecord> indexRecords = Arrays.asList(
                new IndexRecord("record_1", "King Jeremy the Wicked", 0),
                new IndexRecord("record_2", "Lady is a tramp", 1),
                new IndexRecord("record_3", "A baffled king composing hallelujah", 2)
        );

        try (Writer writer = new Writer(analyzer, indexDirectory)) {
            indexRecords.stream().map(IndexRecord::toDocument).forEach(writer::addDocument);
            writer.deleteDocuments(Id.name(), Arrays.asList("record_1"));
        }

        try (Searcher searcher = new Searcher(indexDirectory, analyzer)) {
            String querystr = "King";
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
        }
    }
}
