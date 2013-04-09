package com.nearinfinity.honeycomb.mysql.generators;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

public class QueryKeyGenerator implements Generator<QueryKey> {
    private static final Generator<QueryType> queryTypes =
            PrimitiveGenerators.enumValues(QueryType.class);
    private final Generator<Row> rows;
    private final Generator<String> indices;


    public QueryKeyGenerator(TableSchema schema) {
        super();
        this.rows = new RowGenerator(schema);
        ImmutableList.Builder<String> indices = ImmutableList.builder();
        for (IndexSchema indexSchema : schema.getIndices()) {
            indices.add(indexSchema.getIndexName());
        }
        this.indices = PrimitiveGenerators.fixedValues(indices.build());
    }

    @Override
    public QueryKey next() {
        return new QueryKey(indices.next(), queryTypes.next(), rows.next().getRecords());
    }
}