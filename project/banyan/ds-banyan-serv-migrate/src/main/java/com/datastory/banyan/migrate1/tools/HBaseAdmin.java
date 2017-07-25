package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * com.datastory.banyan.migrate1.tools.HBaseAdmin
 *
 * @author lhfcws
 * @since 2017/6/14
 */
public class HBaseAdmin {
    String[] TABLES = {
            "DS_BANYAN_NEWSFORUM_COMMENT_V1"
    };



    public void dropFamily(String table, String family) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(RhinoETLConfig.getInstance());
             Admin admin = connection.getAdmin()
        ) {
            TableName tblName = TableName.valueOf(table);
            if (admin.tableExists(tblName)) {
                admin.disableTable(tblName);
                admin.deleteColumn(tblName, family.getBytes());
                admin.enableTable(tblName);
            }
        }
    }
}
