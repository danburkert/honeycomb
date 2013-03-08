package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Simple way to decouple the creation of a Store from the user of the store. (May be subject to change)
 */
public class StoreFactory {
    /**
     * Creates a new HBaseStore
     * @return HBaseStore
     */
    public static Store createHBaseStore() throws ParserConfigurationException, SAXException, IOException {
        return HBaseStore.getInstance();
    }
}
