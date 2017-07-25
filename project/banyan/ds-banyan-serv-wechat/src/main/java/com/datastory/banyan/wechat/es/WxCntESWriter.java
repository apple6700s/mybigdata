package com.datastory.banyan.wechat.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;

import java.util.Map;

/**
 * com.datastory.banyan.wechat.es.WxCntESWriter
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxCntESWriter extends BanyanESWriter {
    private static volatile WxCntESWriter _singleton = null;

    public static WxCntESWriter getInstance() {
        if (_singleton == null)
            synchronized (WxCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxCntESWriter();
                }
            }
        return _singleton;
    }

    public static WxCntESWriter getFlushInstance() {
        if (_singleton == null)
            synchronized (WxCntESWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxCntESWriter("ds-banyan-wechat-index-v2");
//                    _singleton.getHooks().clear();
//                    _singleton.getHooks().add(new EsMain2Hook(
//                            new EsRetryLog2Hook(_singleton.getEsBulkClient().getBulkName())
//                    ));
                }
            }
        return _singleton;
    }

    protected WxCntESWriter(String index) {
        super(index, "wechat", 1000);
    }

    protected WxCntESWriter() {
        super(Tables.table(Tables.ES_WECHAT_IDX), "wechat", 1500);
    }

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory.createESBulkProcessorClient(
                        Tables.table(Tables.ES_WECHAT_IDX), "wechat", cacheSize
                );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }

    @Override
    public void write(Map<String, Object> doc) {
        if (!doc.containsKey("_parent"))
            doc.put("_parent", doc.get("biz"));
        super.write(doc);
    }
}
