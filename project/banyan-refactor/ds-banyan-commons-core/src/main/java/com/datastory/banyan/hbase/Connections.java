package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * com.datastory.banyan.hbase.Connections
 *
 * @author lhfcws
 * @since 2017/5/3
 */
public class Connections {
    public static Connection get() throws IOException {
        return ConnectionFactory.createConnection(RhinoETLConfig.getInstance());
    }

    public static Table getTable(Connection connection, String tbl) throws IOException {
        return connection.getTable(TableName.valueOf(tbl));
    }

    public static void close(Table table) throws IOException {
        if (table != null)
            table.close();
    }

    public static void close(Connection connection, Table table) throws IOException {
        close(table);
        if (connection != null)
            connection.close();
    }
}
