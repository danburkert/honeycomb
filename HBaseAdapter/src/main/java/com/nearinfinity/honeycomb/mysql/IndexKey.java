package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.gen.IndexContainer;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexKey {
    private static final DatumWriter<IndexContainer> writer =
            new SpecificDatumWriter<IndexContainer>(IndexContainer.class);
    private static final DatumReader<IndexContainer> reader =
            new SpecificDatumReader<IndexContainer>(IndexContainer.class);
    private final IndexContainer indexContainer;

    public IndexKey(String indexName, QueryType queryType, Map<String, ByteBuffer> keys) {
        checkNotNull(keys);
        this.indexContainer = new IndexContainer(indexName, queryType, keys);
    }

    private IndexKey(IndexContainer indexContainer) {
        this.indexContainer = indexContainer;
    }

    public static IndexKey deserialize(byte[] serializedIndexKey) {
        return new IndexKey(Util.deserializeAvroObject(serializedIndexKey, reader));
    }

    public byte[] serialize() {
        return Util.serializeAvroObject(indexContainer, writer);
    }

    public Map<String, byte[]> getKeys() {
        if (this.indexContainer.getRecords() == null) {
            return null;
        }

        Map<String, byte[]> result = Maps.newHashMap();
        for (Map.Entry<String, ByteBuffer> entry : this.indexContainer.getRecords().entrySet()) {
            result.put(entry.getKey(), entry.getValue().array());
        }

        return result;
    }

    public String getIndexName() {
        return this.indexContainer.getIndexName();
    }

    public QueryType getQueryType() {
        return this.indexContainer.getQueryType();
    }
}
