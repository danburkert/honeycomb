package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.Row;

import java.util.Iterator;

public interface Scanner extends Iterator<Row> {
    /**
     * Closes this scanner and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     */
    public void close();
}
