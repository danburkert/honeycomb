package com.nearinfinity.honeycomb.hbase;


import com.nearinfinity.honeycomb.hbase.gen.PrimaryColumnInfo;
import com.nearinfinity.honeycomb.hbase.gen.PrimaryIndexRow;
import com.nearinfinity.honeycomb.hbase.gen.UUIDBytes;
import com.nearinfinity.honeycomb.mysql.ByteBufferGenerator;
import com.nearinfinity.honeycomb.mysql.UUIDGenerator;
import com.nearinfinity.honeycomb.mysql.Util;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.ListGenerator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class PrimaryIndexRowGenerator implements Generator<PrimaryIndexRow> {
    Generator<Long> longGen = PrimitiveGenerators.longs(0, Long.MAX_VALUE);
    Generator<UUID> uuidGen = new UUIDGenerator();
    Generator<ByteBuffer> valueGen = CombinedGenerators.nullsAnd(new ByteBufferGenerator<ByteBuffer>());
    Generator<List<PrimaryColumnInfo>> columnDetailsGen =
            new ListGenerator(new PrimaryColumnInfoGenerator(), 1, 4);

    @Override
    public PrimaryIndexRow next() {
        return PrimaryIndexRow.newBuilder()
                .setTableId(longGen.next())
                .setUuid(new UUIDBytes(Util.UUIDToBytes(uuidGen.next())))
                .setColumnDetails(columnDetailsGen.next())
                .build();
    }

    private class PrimaryColumnInfoGenerator implements Generator<PrimaryColumnInfo> {
        @Override
        public PrimaryColumnInfo next() {
            return PrimaryColumnInfo.newBuilder()
                    .setColumnId(longGen.next())
                    .setValue(valueGen.next())
                    .build();
        }
    }
}