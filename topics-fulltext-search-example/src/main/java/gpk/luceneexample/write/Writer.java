package gpk.luceneexample.write;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Writer implements IWriter {
    private final IndexWriter indexWriter;
    private final Analyzer analyzer;

    public Writer(Analyzer analyzer, Directory indexDirectory) throws IOException {
        this.analyzer = analyzer;
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        this.indexWriter = new IndexWriter(indexDirectory, config);
    }

    @Override
    public boolean addDocument(Document document) {
        try {
            indexWriter.addDocument(document);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteDocument(String idField, String query) {
        long deleted = deleteDocuments(idField, Collections.singletonList(query));
        return deleted > 0;
    }

    @Override
    public long deleteDocuments(String idfField, List<String> ids) {
        QueryParser queryParser = new QueryParser(idfField, analyzer);
        List<Query> queries = new ArrayList<>();
        try {
            for (String id : ids) {
                queries.add(queryParser.parse(id));
            }
            return indexWriter.deleteDocuments(queries.toArray(new Query[0]));
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public void commit() throws IOException {
        indexWriter.commit();
    }

    @Override
    public void close() {
        try {
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
