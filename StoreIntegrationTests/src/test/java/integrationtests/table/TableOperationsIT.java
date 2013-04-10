package integrationtests.table;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import integrationtests.HoneycombIntegrationTest;
import integrationtests.TestConstants;

import org.junit.Test;

public class TableOperationsIT extends HoneycombIntegrationTest {

    @Test
    public void testRenameTable() {
        final String newTableName = "db2/test2";

        proxy.renameTable(TestConstants.TABLE_NAME, newTableName);
        assertThat(newTableName, equalTo(proxy.getTableName()));

        // Restore the original table name to allow for proper testcase teardown
        proxy.renameTable(newTableName, TestConstants.TABLE_NAME);
    }
}
