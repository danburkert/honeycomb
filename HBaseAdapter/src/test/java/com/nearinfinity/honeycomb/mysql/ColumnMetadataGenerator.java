package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Random;

public class ColumnMetadataGenerator implements Generator<ColumnMetadata> {
    private static Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    private static Generator<Integer> lengthGen = PrimitiveGenerators.integers(0, 65535);
    private static Random rand = new Random();

    @Override
    public ColumnMetadata next() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setType(typeGen.next());

        metadata.setIsNullable(rand.nextBoolean());

        switch(metadata.getType()) {
            case STRING:
            case BINARY:
                metadata.setMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                metadata.setIsAutoincrement(rand.nextBoolean());
                break;
            case DECIMAL:
                int precision = rand.nextInt(66);
                int scale = rand.nextInt(Math.max(31, precision));
                metadata.setPrecision(precision);
                metadata.setScale(scale);
                break;
        }
        return metadata;
    }
}
