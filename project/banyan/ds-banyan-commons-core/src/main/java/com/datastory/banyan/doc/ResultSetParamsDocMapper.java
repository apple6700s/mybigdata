package com.datastory.banyan.doc;

import com.yeezhao.commons.util.Entity.Params;

import java.sql.ResultSet;
import java.util.List;

/**
 * com.datastory.banyan.doc.ResultSetParamsDocMapper
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class ResultSetParamsDocMapper extends ResultSetDocMapper {
    private List<String> cols = null;

    public ResultSetParamsDocMapper(List<String> cols, ResultSet rs) {
        super(rs);
        this.cols = cols;
    }

    @Override
    public Params map() {
        Params p = new Params();
        if (cols != null)
            for (String col : cols) {
                if (!col.equals("_0"))
                    p.put(col, getString(col));
            }
        return p;
    }
}
