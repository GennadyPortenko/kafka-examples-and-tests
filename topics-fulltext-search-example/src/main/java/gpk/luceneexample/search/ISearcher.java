package gpk.luceneexample.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

import java.util.Optional;

public interface ISearcher extends AutoCloseable {
    ScoreDoc[] search(String field, String query);
    Optional<Document> getByDocumentId(int id);
}
