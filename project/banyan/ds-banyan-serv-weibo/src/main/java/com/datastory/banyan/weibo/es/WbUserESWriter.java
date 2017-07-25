package com.datastory.banyan.weibo.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.elasticsearch.action.index.IndexRequest;

/**
 * com.datastory.banyan.weibo.es.WbUserESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WbUserESWriter extends BanyanESWriter {

    private static volatile WbUserESWriter _singleton = null;

    public static WbUserESWriter getInstance() {
        if (_singleton == null)
            synchronized (WbUserESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserESWriter();
                }
            }
        return _singleton;
    }

    public static WbUserESWriter getInstance(String index, int bulkNum) {
        if (_singleton == null)
            synchronized (WbUserESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserESWriter(index, bulkNum);
                }
            }
        return _singleton;
    }

    public WbUserESWriter() {
        super(Tables.table(Tables.ES_WB_IDX), "user", 2000);
    }

    public WbUserESWriter(int bulkNum) {
        super(Tables.table(Tables.ES_WB_IDX), "user", bulkNum);
    }

    public WbUserESWriter(String index, int bulkNum) {
        super(index, "user", bulkNum);
    }

    public static void main(String[] args) throws Exception {
        WbUserESWriter esWriter = WbUserESWriter.getInstance();
        Params p = new Params("id", "6163156195");
        p.put("name", "大叔爱玩");
        p.put("desc", "哇哈哈哈");
        esWriter.write(p, IndexRequest.OpType.CREATE);
//        esWriter.write(p);
        esWriter.close();
        System.exit(0);
    }
}
