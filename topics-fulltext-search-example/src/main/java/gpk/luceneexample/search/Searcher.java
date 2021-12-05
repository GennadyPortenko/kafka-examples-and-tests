package gpk.luceneexample.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Optional;

public class Searcher implements ISearcher {
    private final Analyzer analyzer;
    private final DirectoryReader reader;
    private final IndexSearcher searcher;

    public Searcher(Directory indexDirectory, Analyzer analyzer) throws IOException {
        this.analyzer = analyzer;
        this.reader = DirectoryReader.open(indexDirectory);
        searcher = new IndexSearcher(reader);
    }

    @Override
    public ScoreDoc[] search(String field, String queryStr) {
        final int HITS_PER_PAGE = 10;

        try {
            Query query = new QueryParser(field, analyzer).parse(queryStr);
            TopDocs docs = searcher.search(query, HITS_PER_PAGE);
            return docs.scoreDocs;
        } catch (ParseException | IOException e) {
            e.printStackTrace();
            return new ScoreDoc[0];
        }
    }

    @Override
    public Optional<Document> getByDocumentId(int id) {
        try {
            return Optional.of(searcher.doc(id));
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
