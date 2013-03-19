package com.nearinfinity.honeycomb.mysql.generators;

import java.util.List;
import java.util.Map;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.distribution.Distribution;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public class TableSchemaGenerator implements Generator<TableSchema> {
    /**
     * Maximum number of characters in an identifier
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/identifiers.html">https://dev.mysql.com/doc/refman/5.5/en/identifiers.html</a>
     */
    public static final int MYSQL_MAX_NAME_LENGTH = 64;

    /**
     * Maximum number of columns per table
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/column-count-limit.html">https://dev.mysql.com/doc/refman/5.5/en/column-count-limit.html</a>
     */
    public static final int MYSQL_MAX_COLUMNS = 4096;

    public static final Generator<String> MYSQL_NAME_GEN =
            CombinedGenerators.uniqueValues(PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));

    private static final Generator<ColumnSchema> COLUMN_SCHEMA_GEN = new ColumnSchemaGenerator();

    private static final Generator<Map<String, ColumnSchema>> COLUMNS_GEN =
            CombinedGenerators.maps(MYSQL_NAME_GEN, COLUMN_SCHEMA_GEN,
                    PrimitiveGenerators.integers(1, MYSQL_MAX_COLUMNS, Distribution.POSITIV_NORMAL));

    private static final Generator<Integer> NUM_INDICES_GEN = PrimitiveGenerators.integers(0, 16); // https://dev.mysql.com/doc/refman/5.5/en/column-indexes.html

    @Override
    public TableSchema next() {
        final Map<String, ColumnSchema> columnSchemas = COLUMNS_GEN.next();
        final List<String> columns = Lists.newArrayList(columnSchemas.keySet());

        final Generator<Map<String, IndexSchema>> indexGen = CombinedGenerators.maps(MYSQL_NAME_GEN,
                new IndexSchemaGenerator(columns), NUM_INDICES_GEN);

        return new TableSchema(COLUMNS_GEN.next(), indexGen.next());
    }
}
