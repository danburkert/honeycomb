package com.nearinfinity.honeycomb.mysql;

import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

public class RowTest {

    /**
     * Test that row serialization and deserialization is isomorphic in the
     * serialization direction.
     * @throws Exception
     */
    @Test
    public void testSerDe() throws Exception {
        for(Row row : Iterables.toIterable(new RowGenerator())) {
            Assert.assertEquals(row, Row.deserialize(row.serialize()));
        }
    }
}