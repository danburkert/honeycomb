package com.nearinfinity.honeycomb.mysql;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbase.HBaseStore;

/**
 * Provides test cases for the {@link HandlerProxy} class.
 */
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.nearinfinity.honeycomb.hbase.HBaseStore")
public class HandlerProxyTest {

    private static final String DUMMY_TABLE_NAME = "foo";
    private static final String TABLE_NAME_FIELD = "tableName";

    @Mock
    private HBaseStore storageMock;

    @Mock
    private Table tableMock;


    @Before
    public void setupTests() {
        MockitoAnnotations.initMocks(this);

        PowerMockito.mockStatic(HBaseStore.class);
        when(HBaseStore.getInstance()).thenReturn(storageMock);
    }

    @Test
    public void testRenameTable() throws Exception {
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        final String newTableName = "bar";

        final HandlerProxy proxy = new HandlerProxy(DUMMY_TABLE_NAME);
        proxy.renameTable(newTableName);

        verify(storageMock, times(1)).renameTable(eq(DUMMY_TABLE_NAME), eq(newTableName));
        assertEquals(Whitebox.getInternalState(proxy, TABLE_NAME_FIELD),newTableName);
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullNewTableName() throws Exception {
        new HandlerProxy(DUMMY_TABLE_NAME).renameTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyNewTableName() throws Exception {
        new HandlerProxy(DUMMY_TABLE_NAME).renameTable("");
    }
}
