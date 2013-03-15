package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.Row;

import java.io.Closeable;
import java.util.Iterator;

public interface Scanner extends Closeable, Iterable<Row> {
}
