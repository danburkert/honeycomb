package com.nearinfinity.honeycomb.mysql.generators;

import com.nearinfinity.honeycomb.ColumnSchemaFactory;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
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
        ColumnSchemaBuilder builder = new ColumnSchemaBuilder();
        ColumnType type = typeGen.next();
        builder.withType(type);

        builder.withNullable(RAND.nextBoolean());

        switch (type) {
            case STRING:
            case BINARY:
                builder.withMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                builder.withAutoincrement(RAND.nextBoolean());
                break;
            case DECIMAL:
                int precision = RAND.nextInt(66);
                int scale = RAND.nextInt(Math.max(31, precision));
                builder.withScalePrecision(scale, precision);
                break;
            default:
                break;
        }

        return builder.build();
    }

    private class ColumnSchemaBuilder {
        private ColumnType type;
        private int precision, scale;
        private boolean autoIncrement, nullable;
        private int maxLength;

        public ColumnSchemaBuilder withScalePrecision(int scale, int precision) {
            this.scale = scale;
            this.precision = precision;
            return this;
        }

        public ColumnSchemaBuilder withType(ColumnType type) {
            this.type = type;
            return this;
        }

        public ColumnSchemaBuilder withAutoincrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public ColumnSchemaBuilder withMaxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public ColumnSchemaBuilder withNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public ColumnSchema build() {
            return ColumnSchemaFactory.createColumnSchema(type, nullable, autoIncrement, maxLength, scale, precision, "default" + RAND.nextLong());
        }
    }
}
