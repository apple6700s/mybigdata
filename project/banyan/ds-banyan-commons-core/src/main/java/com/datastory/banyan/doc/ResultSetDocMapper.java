package com.datastory.banyan.doc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * com.datastory.banyan.doc.ResultSetDocMapper
 *
 * @author lhfcws
 * @since 2016/10/25
 */
public abstract class ResultSetDocMapper extends DocMapper {
    protected ResultSet rs;
    public ResultSetDocMapper(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public String getString(String key) {
        try {
            return rs.getString(key);
        } catch (SQLException e) {
            return null;
        }
    }

    @Override
    public Integer getInt(String key) {
        try {
            return rs.getInt(key);
        } catch (SQLException e) {
            return null;
        }
    }

    public void close() {
        if (rs != null)
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
            }
    }
}
