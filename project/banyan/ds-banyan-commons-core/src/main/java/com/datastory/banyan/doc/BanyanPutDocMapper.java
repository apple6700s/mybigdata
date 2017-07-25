package com.datastory.banyan.doc;

import com.datastory.banyan.base.RhinoETLConsts;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.doc.BanyanPutDocMapper
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class BanyanPutDocMapper extends PutRDocMapper {
    static byte[] F = "f".getBytes();

    public BanyanPutDocMapper(Put put) {
        super(put);
    }

    @Override
    public Params map() {
        Params p = super.map();
        if (p == null)
            return p;
        if (put.getFamilyCellMap().containsKey(F)) {
            List<Cell> cells = put.getFamilyCellMap().get(F);
            List<String> flist = new LinkedList<>();
            if (cells != null) {
                for (Cell cell : cells) {
                    flist.add(Bytes.toString(cell.getQualifier()));
                }
            }
            if (!flist.isEmpty()) {
                p.put(RhinoETLConsts.FOLLOW_LIST_KEY, flist);
            }
        }
        return p;
    }
}
