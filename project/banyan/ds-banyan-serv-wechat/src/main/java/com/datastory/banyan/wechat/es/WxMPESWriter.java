package com.datastory.banyan.wechat.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;

/**
 * com.datastory.banyan.wechat.es.WxCntESWriter
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxMPESWriter extends BanyanESWriter {
    private static volatile WxMPESWriter _singleton = null;

    public static WxMPESWriter getInstance() {
        if (_singleton == null)
            synchronized (WxMPESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxMPESWriter();
                }
            }
        return _singleton;
    }

    protected WxMPESWriter() {
        super(Tables.table(Tables.ES_WECHAT_IDX), "mp", 3000);
    }

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory.createESBulkProcessorClient(
                        Tables.table(Tables.ES_WECHAT_IDX), "mp", cacheSize
                );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }
}
