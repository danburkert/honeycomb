package integrationtests.autoinc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import integrationtests.HoneycombIntegrationTest;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class AutoIncrementOperationsIT extends HoneycombIntegrationTest {

    @Override
    protected TableSchema getTableSchema() {
        final List<ColumnSchema> columns = Lists.newArrayList();
        columns.add(ColumnSchema.builder("test", ColumnType.LONG).build());
        return new TableSchema(columns, ImmutableList.<IndexSchema>of());
    }

    @Test
    public void testGetAutoIncrement() {
        assertThat(proxy.getAutoIncrement(), equalTo(1L));
    }

    @Test
    public void testIncrementAutoIncrement() {
        assertThat(proxy.incrementAutoIncrement(3), equalTo(1L));

        assertThat(proxy.incrementAutoIncrement(1), equalTo(4L));
    }

    @Test
    public void testTruncateAutoInc() {
        proxy.truncateAutoIncrement();
        assertThat(proxy.getAutoIncrement(), equalTo(1L));
    }
}
