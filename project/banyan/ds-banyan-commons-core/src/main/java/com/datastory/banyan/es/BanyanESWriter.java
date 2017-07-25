package com.datastory.banyan.es;

import com.datastory.banyan.base.RhinoETLConfig;

/**
 * com.datastory.banyan.es.ESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class BanyanESWriter extends ESWriter {

    public BanyanESWriter(String indexName, String indexType) {
        this(indexName, indexType, DFT_MAX_BULK_NUM);
    }

    public BanyanESWriter(String indexName, String indexType, int bulkNum) {
        super(indexName, indexType, bulkNum);
        initEsWriteHooks();
    }

    public void initEsWriteHooks() {
        if (RhinoETLConfig.getInstance().getBoolean("banyan.es.hooks.enable", true)) {
            getEsBulkClient().getHooks().add(getEsBulkClient().getMainHook());
        }
    }

}
