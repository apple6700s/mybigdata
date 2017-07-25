package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.BanyanPutDocMapper;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.monitor.Status;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Put;
import org.elasticsearch.action.index.IndexRequest;

import java.util.List;

/**
 * com.datastory.banyan.weibo.hbase.WbCntHBaseWriter
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class WbUserHBaseWriter extends BanyanRFieldPutter {
    static final String table = Tables.table(Tables.PH_WBUSER_TBL);

    public WbUserHBaseWriter() {
        super(table);
    }

    public WbUserHBaseWriter(int cacheSize) {
        super(table, cacheSize);
    }

    private static volatile WbUserHBaseWriter _singleton = null;

    public static WbUserHBaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (WbUserHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserHBaseWriter();
                }
            }
        }
        return _singleton;
    }

    public static WbUserHBaseWriter getInstance(int num) {
        if (_singleton == null) {
            synchronized (WbUserHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserHBaseWriter(num);
                }
            }
        }
        return _singleton;
    }

    protected void init() {
        super.init();
//        if (RhinoETLConfig.getInstance().getBoolean("enable.es.writer", true))
//        setEsWriterHook(WbUserESWriter.getInstance(), WbUser2RhinoESDocMapper.class);
    }

    public void esWriterHook(List<Put> clist, int res) {
        if (clist == null || clist.size() == 0 || esWriter == null || !Status.isHBaseSuccess(res))
            return;

        ESWriterAPI writer = esWriter;
        try {
            ParamsDocMapper dm = null;
            if (docMapperClass != null)
                dm = ClassUtil.newInstance(docMapperClass, new Params());

            for (Put put : clist) {
                Params hbDoc = new BanyanPutDocMapper(put).map();
                if (dm != null) {
                    dm.setIn(hbDoc);
                    Params esDoc = (Params) dm.map();
                    if (esDoc != null)
                        writer.write(esDoc, IndexRequest.OpType.CREATE);
                } else
                    LOG.error("Cannot find docmapper for :   " + hbDoc);
            }
        } catch (Exception e) {
            LOG.error(e);
        } finally {
            writer.flush();
        }
    }
}
