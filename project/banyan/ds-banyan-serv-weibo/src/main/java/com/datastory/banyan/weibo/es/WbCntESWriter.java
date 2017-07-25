package com.datastory.banyan.weibo.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.es.hooks.EsMain2Hook;
import com.datastory.banyan.es.hooks.EsRetry2Hook;
import com.datastory.banyan.es.hooks.EsRetryLog2Hook;
import com.datastory.banyan.utils.FactoryFunc;

import java.util.Map;

/**
 * com.datastory.banyan.weibo.es.WbCntESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WbCntESWriter extends BanyanESWriter {

    private static volatile WbCntESWriter _singleton = null;

    public static WbCntESWriter getInstance() {
        if (_singleton == null)
            synchronized (WbCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntESWriter();
                }
            }
        return _singleton;
    }

    public static WbCntESWriter getInstance(int bulkNum) {
        if (_singleton == null)
            synchronized (WbCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntESWriter(bulkNum);
                }
            }
        return _singleton;
    }

    public static WbCntESWriter getInstance(String index, int bulkNum) {
        if (_singleton == null)
            synchronized (WbCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntESWriter(index, bulkNum);
                }
            }
        return _singleton;
    }

    public static WbCntESWriter getFlushInstance(String index, int bulkNum) {
        if (_singleton == null)
            synchronized (WbCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntESWriter(index, bulkNum);
                    _singleton.getHooks().clear();
                    _singleton.getHooks().add(new EsMain2Hook(
                            new EsRetry2Hook((BanyanEsBulkClient) _singleton.getEsBulkClient()),
                            new EsRetryLog2Hook(_singleton.getEsBulkClient().getBulkName())
                    ));
                }
            }
        return _singleton;
    }

    public WbCntESWriter() {
        super(Tables.table(Tables.ES_WB_IDX), "weibo", 5000);
    }

    public WbCntESWriter(int bulkNum) {
        super(Tables.table(Tables.ES_WB_IDX), "weibo", bulkNum);
    }

    public WbCntESWriter(String index, int bulkNum) {
        super(index, "weibo", bulkNum);
    }

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory
//                        .createOneNodeSimpleClient(
                        .createESBulkProcessorClient(
                                Tables.table(Tables.ES_WB_IDX), "weibo", cacheSize
                        );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }

    @Override
    public void write(Map<String, Object> doc) {
        if (!doc.containsKey("_parent"))
            doc.put("_parent", doc.get("uid"));
        super.write(doc);
    }
}
