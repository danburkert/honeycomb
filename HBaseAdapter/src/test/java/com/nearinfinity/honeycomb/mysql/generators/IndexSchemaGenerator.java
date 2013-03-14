package com.nearinfinity.honeycomb.mysql.generators;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;

public class IndexSchemaGenerator implements Generator<IndexSchema> {
    private static final Generator<Integer> lengthGen = PrimitiveGenerators.integers(1, 4);
    private static final Random RAND = new Random();
    private final List<String> columnNames;


    public IndexSchemaGenerator(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public IndexSchema next() {
        Collections.shuffle(columnNames);
        List<String> columns = columnNames.subList(0, Math.min(lengthGen.next(), columnNames.size()));
        return new IndexSchema(columns, RAND.nextBoolean());
    }
}