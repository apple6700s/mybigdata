package com.datastory.banyan.hbase;

import com.datastory.banyan.doc.ResultSetParamsDocMapper;
import com.yeezhao.commons.util.Entity.Params;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.hbase.PhoenixReader
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class PhoenixReader implements Serializable {
    private PhoenixDriver driver = PhoenixDriverFactory.getDriver();

    public List<Params> query(String sql) throws SQLException {
        List<Params> ret = new LinkedList<>();
        List<String> colnames = null;
        ResultSet rs = driver.query(sql);
        while (rs.next()) {
            if (colnames == null) {
                colnames = PhoenixDriver.getAllColumnNames(rs.getMetaData());
            }
            Params p = new ResultSetParamsDocMapper(colnames, rs).map();
            ret.add(p);
        }
        return ret;
    }

    public List<String> queryField(String sql, String field) throws SQLException {
        List<String> ret = new LinkedList<>();
        List<String> colnames = Collections.singletonList(field);
        ResultSet rs = driver.query(sql);
        while (rs.next()) {
            Params p = new ResultSetParamsDocMapper(colnames, rs).map();
            ret.add(p.getString(field));
        }
        return ret;
    }
}
