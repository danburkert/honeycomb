package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.distribution.Distribution;

import java.util.Map;

public class TableSchemaGenerator implements Generator<TableSchema> {
    static final int MYSQL_MAX_NAME_LENGTH = 64; // https://dev.mysql.com/doc/refman/5.5/en/identifiers.html
    static final int MYSQL_MAX_COLUMNS = 4096; // https://dev.mysql.com/doc/refman/5.5/en/column-count-limit.html
    private static Generator<String> nameGen =
            CombinedGenerators.uniqueValues(
                    PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));
    private static Generator<ColumnSchema> columnGen =
            new ColumnSchemaGenerator();
    private static Generator<Map<String, ColumnSchema>> columnsGen =
            CombinedGenerators.maps(nameGen, columnGen,
                    PrimitiveGenerators.integers(1, MYSQL_MAX_COLUMNS, Distribution.POSITIV_NORMAL));

    @Override
    public TableSchema next() {
        return new TableSchema(nameGen.next(), columnsGen.next());
    }
}