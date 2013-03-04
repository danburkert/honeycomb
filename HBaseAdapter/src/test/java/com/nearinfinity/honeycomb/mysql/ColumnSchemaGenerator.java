package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Random;

public class ColumnSchemaGenerator implements Generator<ColumnSchema> {
    private static Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    private static Generator<Integer> lengthGen = PrimitiveGenerators.integers(0, 65535);
    private static Random rand = new Random();

    @Override
    public ColumnSchema next() {
        ColumnSchema schema = new ColumnSchema();
        schema.setType(typeGen.next());

        schema.setIsNullable(rand.nextBoolean());

        switch(schema.getType()) {
            case STRING:
            case BINARY:
                schema.setMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                schema.setIsAutoincrement(rand.nextBoolean());
                break;
            case DECIMAL:
                int precision = rand.nextInt(66);
                int scale = rand.nextInt(Math.max(31, precision));
                schema.setPrecision(precision);
                schema.setScale(scale);
                break;
        }
        return schema;
    }
}
