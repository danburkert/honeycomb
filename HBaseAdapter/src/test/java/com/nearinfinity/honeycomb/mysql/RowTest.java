package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import com.nearinfinity.honeycomb.mysql.generators.RowGenerator;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

public class RowTest {

    /**
     * Test that row serialization and deserialization is isomorphic in the
     * serialization direction.
     *
     * @throws Exception
     */
    @Test
    public void testSerDe() throws Exception {
        TableSchema schema = new TableSchemaGenerator().next();
        for (Row row : Iterables.toIterable(new RowGenerator(schema))) {
            Assert.assertEquals(row, Row.deserialize(row.serialize()));
        }
    }
}