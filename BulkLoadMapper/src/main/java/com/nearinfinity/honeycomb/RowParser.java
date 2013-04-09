package com.nearinfinity.honeycomb;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

public class RowParser {
    private final TableSchema schema;
    private final String[] columns;
    private final CSVParser csvParser;

    public RowParser(TableSchema schema, String[] columns, char separator) {
        this.schema = schema;
        this.columns = columns;
        this.csvParser = new CSVParser(separator);
    }

    public Row parseRow(String line) throws IOException, ParseException {
        String[] unparsedFields = csvParser.parseLine(line);
        checkArgument((schema.getColumns().size() == unparsedFields.length),
                "Line contains wrong number of columns.");

        ImmutableMap.Builder<String, ByteBuffer> fields = ImmutableMap.builder();

        for (int i = 0; i < columns.length; i++) {
            fields.put(columns[i],
                    FieldParser.parse(unparsedFields[i],
                            schema.getColumnSchema(columns[i])));
        }
        return new Row(fields.build(), UUID.randomUUID());
    }
}