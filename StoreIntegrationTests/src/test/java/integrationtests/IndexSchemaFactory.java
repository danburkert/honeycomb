package integrationtests;

import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        return new IndexSchema(columns, isUnique, indexName);
    }
}
