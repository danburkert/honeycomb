package integrationtests;

import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        return new IndexSchema(columns, isUnique, indexName);
    }
}
