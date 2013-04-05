package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.AvroQueryKey;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexKey {
    private static final DatumWriter<AvroQueryKey> writer =
            new SpecificDatumWriter<AvroQueryKey>(AvroQueryKey.class);
    private static final DatumReader<AvroQueryKey> reader =
            new SpecificDatumReader<AvroQueryKey>(AvroQueryKey.class);
    private final AvroQueryKey avroQueryKey;

    public IndexKey(String indexName, QueryType queryType, Map<String, ByteBuffer> keys) {
        checkNotNull(keys);
        this.avroQueryKey = new AvroQueryKey(indexName, queryType, keys);
    }

    private IndexKey(AvroQueryKey AvroQueryKey) {
        this.avroQueryKey = AvroQueryKey;
    }

    public static IndexKey deserialize(byte[] serializedIndexKey) {
        return new IndexKey(Util.deserializeAvroObject(serializedIndexKey, reader));
    }

    public byte[] serialize() {
        return Util.serializeAvroObject(avroQueryKey, writer);
    }

    public Map<String, ByteBuffer> getKeys() {
        return this.avroQueryKey.getRecords();
    }

    public String getIndexName() {
        return this.avroQueryKey.getIndexName();
    }

    public QueryType getQueryType() {
        return this.avroQueryKey.getQueryType();
    }
}
