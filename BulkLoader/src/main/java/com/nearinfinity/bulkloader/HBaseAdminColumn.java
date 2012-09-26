package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseAdminColumn {
    public static void addDummyFamily(HBaseAdmin admin) throws IOException, InterruptedException {
        final HColumnDescriptor dummyColumn = new HColumnDescriptor(SamplingReducer.DUMMY_FAMILY);
        HTableDescriptor sqlTableDescriptor = admin.getTableDescriptor(Constants.SQL);
        changeTable(admin, sqlTableDescriptor, new Command() {
            @Override
            public void run(HBaseAdmin admin) throws IOException {
                admin.addColumn(Constants.SQL, dummyColumn);
            }
        });
    }

    public static void deleteDummyFamily(HBaseAdmin admin) throws IOException, InterruptedException {
        HTableDescriptor sqlTableDescriptor = admin.getTableDescriptor(Constants.SQL);
        changeTable(admin, sqlTableDescriptor, new Command() {
            @Override
            public void run(HBaseAdmin admin) throws IOException {
                admin.deleteColumn(Constants.SQL, SamplingReducer.DUMMY_FAMILY);
            }
        });
    }

    private static void changeTable(HBaseAdmin admin, HTableDescriptor sqlTableDescriptor, Command modification) throws IOException, InterruptedException {
        if (!sqlTableDescriptor.hasFamily(SamplingReducer.DUMMY_FAMILY)) {
            if (!admin.isTableDisabled(Constants.SQL)) {
                admin.disableTable(Constants.SQL);
            }

            modification.run(admin);
        }

        if (admin.isTableDisabled(Constants.SQL)) {
            admin.enableTable(Constants.SQL);
        }

        admin.flush(Constants.SQL);
    }

    // Java is so stupid without closures.
    private interface Command {
        void run(HBaseAdmin admin) throws IOException;
    }
}
