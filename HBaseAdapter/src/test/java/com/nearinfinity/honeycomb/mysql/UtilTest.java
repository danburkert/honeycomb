package com.nearinfinity.honeycomb.mysql;

import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;

import java.util.UUID;

public class UtilTest {
    @Test
    public void testUUIDBytes() throws Exception {
        for (UUID uuid : Iterables.toIterable(new UUIDGenerator())) {
            Assert.assertEquals(uuid, Util.BytesToUUID(Util.UUIDToBytes(uuid)));
        }
    }
}