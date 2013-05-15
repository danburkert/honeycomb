package com.nearinfinity.honeycomb.mysql;

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.mysql.gen.AvroQueryKey;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class QueryKey {
    private static final DatumWriter<AvroQueryKey> writer =
            new SpecificDatumWriter<AvroQueryKey>(AvroQueryKey.class);
    private static final DatumReader<AvroQueryKey> reader =
            new SpecificDatumReader<AvroQueryKey>(AvroQueryKey.class);
    private final AvroQueryKey avroQueryKey;

    public QueryKey(String indexName, QueryType queryType, Map<String, ByteBuffer> fields) {
        checkNotNull(fields);
        checkNotNull(queryType);
        Verify.isNotNullOrEmpty(indexName);

        this.avroQueryKey = new AvroQueryKey(indexName, queryType, fields);
    }

    private QueryKey(AvroQueryKey AvroQueryKey) {
        this.avroQueryKey = AvroQueryKey;
    }

    public static QueryKey deserialize(byte[] serializedIndexKey) {
        checkNotNull(serializedIndexKey);
        return new QueryKey(Util.deserializeAvroObject(serializedIndexKey, reader));
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

    @Override
    public String toString() {
        final Objects.ToStringHelper toString = Objects.toStringHelper(this.getClass());

        toString.add("Index", avroQueryKey.getIndexName())
                .add("Type", avroQueryKey.getQueryType());

        for (final Map.Entry<String, ByteBuffer> entry : getKeys().entrySet()) {
            toString.add("Key", format("%s: %s", entry.getKey(), entry.getValue()));
        }

        return toString.toString();
    }
}
