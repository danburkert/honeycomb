package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.distribution.Distribution;

import java.util.Map;

public class TableMetadataGenerator implements Generator<TableMetadata> {
    static final int MYSQL_MAX_NAME_LENGTH = 64; // https://dev.mysql.com/doc/refman/5.5/en/identifiers.html
    static final int MYSQL_MAX_COLUMNS = 4096; // https://dev.mysql.com/doc/refman/5.5/en/column-count-limit.html
    private static Generator<String> nameGen =
            CombinedGenerators.uniqueValues(
                    PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));
    private static Generator<ColumnMetadata> columnGen =
            new ColumnMetadataGenerator();
    private static Generator<Map<String, ColumnMetadata>> columnsGen =
            CombinedGenerators.maps(nameGen, columnGen,
                    PrimitiveGenerators.integers(1, MYSQL_MAX_COLUMNS, Distribution.POSITIV_NORMAL));

    @Override
    public TableMetadata next() {
        return new TableMetadata(nameGen.next(), columnsGen.next());
    }
}