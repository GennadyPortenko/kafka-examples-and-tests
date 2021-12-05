package gpk.luceneexample.data;

import lombok.Data;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import java.util.HashMap;
import java.util.Map;

@Data
public class IndexRecord {
    private final Map<String, String> fields = new HashMap<>();
    final private String key;
    final private String value;
    final private long timestamp;

    public Document toDocument() {
        Document document = new Document();
        document.add(new StringField(RecordField.Id.name(), key, Field.Store.YES));
        document.add(new TextField(RecordField.Key.name(), key, Field.Store.YES));
        document.add(new TextField(RecordField.Value.name(), value, Field.Store.YES));
        document.add(new StringField(RecordField.Timestamp.name(), String.valueOf(timestamp), Field.Store.YES));
        return document;
    }

    public enum RecordField {
        Id,
        Key,
        Value,
        Timestamp
    }
}
