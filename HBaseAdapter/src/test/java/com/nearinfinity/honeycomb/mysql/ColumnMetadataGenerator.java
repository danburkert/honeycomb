package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

public class ColumnMetadataGenerator implements Generator<ColumnMetadata> {
    private static Generator<String> nameGen = PrimitiveGenerators.strings();
    private static Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    @Override
    public ColumnMetadata next() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setName(nameGen.next());
        ColumnType type = typeGen.next();
//        switch(type) {
//            case STRING:
//
//        }
        return metadata;

    }
}
