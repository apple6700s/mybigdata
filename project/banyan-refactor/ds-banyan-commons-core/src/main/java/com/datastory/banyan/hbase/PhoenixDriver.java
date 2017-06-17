package com.datastory.banyan.hbase;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;
import java.util.*;


/**
 * com.datastory.banyan.hbase.PhoenixDriver
 * <p>
 * Thread-safe with immutable attrs.
 *
 * @author sugan
 * @since 2015-12-22
 */
public class PhoenixDriver implements Serializable {
    private static Logger LOG = Logger.getLogger(PhoenixDriver.class);
    private String connUri;
    private Properties properties;

    static {
        String driverName = "org.apache.phoenix.jdbc.PhoenixDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public PhoenixDriver(String zkQuorum, int zkPort, String hbaseZkPath) {
        this(zkQuorum, zkPort, hbaseZkPath, null);
    }

    public PhoenixDriver(String zkQuorum, int zkPort, String hbaseZkPath, Properties properties) {
        // path:  /hbase-unsecure
        // zk: localhost:2181
        this.connUri = String.format("jdbc:phoenix:%s:%s:%s", zkQuorum, zkPort, hbaseZkPath);
        //this.jedisUtil = JedisUtil.getInstance(RhinoETLConfig.getInstance());
        this.properties = properties;
        LOG.info("phoenix connection uri:" + connUri);
    }

    private Connection getConnection() throws SQLException {
        Connection con;
        if (properties != null) {
            con = DriverManager.getConnection(connUri, properties);

            LOG.info("phoenix properties:" + properties);
        } else
            con = DriverManager.getConnection(connUri);
        con.setAutoCommit(false);
        return con;
    }

    public ResultSet query(String sql) throws SQLException {
        System.out.println("[SQL] " + sql);

        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(true);
            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            return rs;
        } catch (Throwable t) {
            if (statement != null)
                statement.close();
            if (conn != null)
                conn.close();
            throw new SQLException(sql + "  || " + t);
        }
    }

    public int[] batchExecute(String sql, Object... params) throws SQLException {
        return batchExecute(sql, Collections.singletonList(params));
    }

    public <T extends Object> int[] batchExecute(String sql, List<T[]> params) throws SQLException {
        int[] rows = new int[0];
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getConnection();
            stmt = con.prepareStatement(sql);
            for (Object[] param : params) {
                fillStatement(stmt, param);
                stmt.addBatch();
                stmt.clearParameters();
            }
            rows = stmt.executeBatch();
            con.commit();
        } catch (Throwable e) {
            LOG.error("[SAMPLE] " + Arrays.toString(params.get(0)) +"\nerr sql: " + sql + " , " + e.getMessage(), e);
        } finally {
            if (stmt != null)
                stmt.close();
            if (con != null)
                con.close();
        }
        return rows;
    }

    public void fillStatement(PreparedStatement stmt, Object... params) throws SQLException {
        ParameterMetaData pmd = stmt.getParameterMetaData();
        int stmtCount = pmd.getParameterCount();
        int paramsCount = params == null ? 0 : params.length;

        if (stmtCount != paramsCount) {
            if (params == null) {
                System.out.println("params is null.");
                LOG.error("Params is null.");
            } else {
                System.out.println(Arrays.asList(params));
                LOG.error(Arrays.asList(params));
            }
            throw new SQLException("Wrong number of parameters: expected "
                    + stmtCount + ", was given " + paramsCount);
        }

        if (params == null) {
            return;
        }

        for (int i = 0; i < params.length; i++) {
            if (params[i] != null) {
                stmt.setObject(i + 1, params[i]);
            } else {
                int sqlType = Types.VARCHAR;
                try {
                    sqlType = pmd.getParameterType(i + 1);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                stmt.setNull(i + 1, sqlType);
            }
        }
    }

    public List<String> getAllColumnNames(String table) throws SQLException {
        int retry = 5;
        Exception sqlE = null;
        while (retry-- >= 0) {
            try {
                ResultSet rs = this.query("SELECT * FROM \"" + table + "\" limit 1");
                return getAllColumnNames(rs.getMetaData());
            } catch (SQLException e) {
                sqlE = e;
            }
        }
        SQLException e = new SQLException("no fields of table " + table + " . " + sqlE.getMessage());
        e.setStackTrace(sqlE.getStackTrace());
        throw e;
    }

    public static List<String> getAllColumnNames(ResultSetMetaData metaData) throws SQLException {
        int colcnt = metaData.getColumnCount();
        List<String> cols = new LinkedList<>();
        for (int i = 1; i <= colcnt; i++) {
            cols.add(metaData.getColumnName(i));
        }
        return cols;
    }
}