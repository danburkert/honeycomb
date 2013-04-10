package integrationtests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

public final class TestConstants {
    public static final String COLUMN1 = "c1";
    public static final String COLUMN2 = "c2";
    public static final String INDEX3 = "i3";
    public static final String INDEX2 = "i2";
    public static final String INDEX1 = "i1";
    public static final String TABLE_NAME = "db/test";

    public static final TableSchema TABLE_SCHEMA =
            new TableSchema(
                    ImmutableList.of(
                            ColumnSchema.builder(COLUMN1, ColumnType.LONG).build(),
                            ColumnSchema.builder(COLUMN2, ColumnType.LONG).build()
                    ),
                    ImmutableList.of(
                            new IndexSchema(TestConstants.INDEX1,
                                    Lists.newArrayList(TestConstants.COLUMN1), false),
                            new IndexSchema(TestConstants.INDEX2,
                                    Lists.newArrayList(TestConstants.COLUMN1,
                                            TestConstants.COLUMN2), false)
                    )
            );
}
