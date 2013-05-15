package com.nearinfinity.honeycomb.mysql.generators;

import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Random;

public class ColumnSchemaGenerator implements Generator<ColumnSchema> {
    //    private static final int MYSQL_MAX_VARCHAR = 65535;
    private static final int MYSQL_MAX_VARCHAR = 128; // Set artificially low so tests are fast
    private static final Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    private static final Generator<Integer> lengthGen = PrimitiveGenerators.integers(0, MYSQL_MAX_VARCHAR);
    private static final Random RAND = new Random();

    /**
     * Maximum number of characters in an identifier
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/identifiers.html">https://dev.mysql.com/doc/refman/5.5/en/identifiers.html</a>
     */
    public static final int MYSQL_MAX_NAME_LENGTH = 64;
    public static final Generator<String> MYSQL_NAME_GEN =
            CombinedGenerators.uniqueValues(PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));

    @Override
    public ColumnSchema next() {
        ColumnType type = typeGen.next();
        ColumnSchema.Builder builder = ColumnSchema.builder(MYSQL_NAME_GEN.next(), type);
        switch (type) {
            case STRING:
            case BINARY:
                builder.setMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                builder.setIsAutoIncrement(RAND.nextBoolean());
                break;
            case DECIMAL:
                int precision = RAND.nextInt(66);
                int scale = RAND.nextInt(Math.max(31, precision));
                builder.setPrecision(precision).setScale(scale);
                break;
            default:
                break;
        }
        return builder.build();
    }
}
