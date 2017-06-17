package com.datastory.banyan.weibo.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;

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

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory.createSimpleClient(
                        Tables.table(Tables.ES_WB_IDX), "user", cacheSize
                );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }
}
