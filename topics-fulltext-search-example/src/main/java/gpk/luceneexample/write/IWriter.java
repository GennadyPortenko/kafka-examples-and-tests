package gpk.luceneexample.write;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;

public interface IWriter extends AutoCloseable{
    boolean addDocument(Document document);
    long deleteDocuments(String idField, List<String> queries);
    boolean deleteDocument(String idField, String query);
    void commit() throws IOException;
}
