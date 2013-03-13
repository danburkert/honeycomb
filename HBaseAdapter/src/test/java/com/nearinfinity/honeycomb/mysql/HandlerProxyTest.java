package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Provides test cases for the {@link HandlerProxy} class.
 */
@RunWith(PowerMockRunner.class)
public class HandlerProxyTest {

    private static final String DUMMY_TABLE_NAME = "foo";
    private static final String DUMMY_DATABASE_NAME = "hbase";
    @Mock
    private HBaseStore storageMock;
    @Mock
    private StoreFactory storeFactory;
    @Mock
    private Table tableMock;

    @Before
    public void setupTests() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRenameTable() throws Exception {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        final String renamedTableName = "bar";

        final HandlerProxy proxy = createProxy();
        String oldTableName = Util.fullyQualifyTable(DUMMY_DATABASE_NAME, DUMMY_TABLE_NAME);
        String newTableName = Util.fullyQualifyTable(DUMMY_DATABASE_NAME, renamedTableName);
        proxy.openTable(DUMMY_DATABASE_NAME, DUMMY_TABLE_NAME, "tablespace");
        proxy.renameTable(DUMMY_DATABASE_NAME, renamedTableName);

        verify(storageMock, times(1)).renameTable(eq(oldTableName), eq(newTableName));
        assertEquals(proxy.getTableName(), newTableName);
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullNewTableName() throws Exception {
        createProxy().renameTable(DUMMY_DATABASE_NAME, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyNewTableName() throws Exception {
        createProxy().renameTable(DUMMY_DATABASE_NAME, "");
    }

    private HandlerProxy createProxy() throws Exception {
        return new HandlerProxy(storeFactory);
    }
}
