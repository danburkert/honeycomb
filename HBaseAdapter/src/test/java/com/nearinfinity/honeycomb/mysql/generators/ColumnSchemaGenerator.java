package com.nearinfinity.honeycomb.mysql.generators;

import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Random;

public class ColumnSchemaGenerator implements Generator<ColumnSchema> {
//    private static final int MYSQL_MAX_VARCHAR = 65535;
    private static final int MYSQL_MAX_VARCHAR = 128; // Set artificially low so tests are fast

    private static final Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    private static final Generator<Integer> lengthGen = PrimitiveGenerators.integers(0, MYSQL_MAX_VARCHAR);
    private static final Random RAND = new Random();

    @Override
    public ColumnSchema next() {
        ColumnSchema schema = new ColumnSchema();
        schema.setType(typeGen.next());

        schema.setIsNullable(RAND.nextBoolean());

        switch(schema.getType()) {
            case STRING:
            case BINARY:
                schema.setMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                schema.setIsAutoIncrement(RAND.nextBoolean());
                break;
            case DECIMAL:
                int precision = RAND.nextInt(66);
                int scale = RAND.nextInt(Math.max(31, precision));
                schema.setPrecision(precision);
                schema.setScale(scale);
                break;
            default:
                break;
        }

        return schema;
    }
}
