package com.datastory.banyan.doc;

import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * com.datastory.banyan.doc.PutRDocMapper
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class PutRDocMapper extends DocMapper {
    static final byte[] R = "r".getBytes();

    Put put;

    public PutRDocMapper(Put put) {
        this.put = put;
    }

    @Override
    public String getString(String key) {
        return null;
    }

    @Override
    public Integer getInt(String key) {
        return null;
    }

    @Override
    public Params map() {
        List<Cell> cells = put.getFamilyCellMap().get(R);
        if (cells == null) return null;

        Params p = new Params();
        for (Cell cell :cells) {
            p.put(Bytes.toString(cell.getQualifier()), Bytes.toString(cell.getValue()));
        }
        p.put("pk", Bytes.toString(put.getRow()));

        return p;
    }
}
