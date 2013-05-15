package integrationtests.rowcount;

import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import integrationtests.HoneycombIntegrationTest;
import integrationtests.ITUtils;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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

    @Test
    public void testIncrementRowCountConcurrently() throws Exception{
        final long amount = 13;
        final int concurrency = 8;
        final long expectedRowCount = amount * concurrency;
        ITUtils.startProxyActionConcurrently(
                concurrency,
                ITUtils.openTable,
                new IncrementRowCount(amount),
                ITUtils.closeTable,
                factory);
        assertEquals(expectedRowCount, proxy.getRowCount());
    }

    private class IncrementRowCount implements ITUtils.ProxyRunnable {
        private long amount;
        public IncrementRowCount(long amount) {
            this.amount = amount;
        }
        @Override
        public void run(HandlerProxy proxy) {
            proxy.incrementRowCount(amount);
        }
    }
}