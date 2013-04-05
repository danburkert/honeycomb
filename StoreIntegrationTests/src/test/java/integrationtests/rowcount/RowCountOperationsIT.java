package integrationtests.rowcount;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import integrationtests.HoneycombIntegrationTest;

import org.junit.Test;

public class RowCountOperationsIT extends HoneycombIntegrationTest {

    @Test
    public void testGetRowCount() {
        proxy.incrementRowCount(2);

        assertThat(proxy.getRowCount(), equalTo(2L));
    }

    @Test
    public void testTruncateRowCount() {
        proxy.incrementRowCount(5);
        proxy.truncateRowCount();

        assertThat(proxy.getRowCount(), equalTo(0L));
    }
}
