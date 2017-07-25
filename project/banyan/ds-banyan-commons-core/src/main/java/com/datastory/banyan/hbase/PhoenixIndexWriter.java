package com.datastory.banyan.hbase;

import com.datastory.banyan.utils.BanyanTypeUtil;

import java.util.List;

/**
 * com.datastory.banyan.hbase.PhoenixIndexWriter
 *
 * @author lhfcws
 * @since 16/11/30
 */
@Deprecated
public class PhoenixIndexWriter extends PhoenixWriter {
    private String table;
    public PhoenixIndexWriter(String table) {
        super();
        this.table = table;
    }

    @Override
    protected boolean isLazyInit() {
        return true;
    }

    @Override
    public String getTable() {
        return this.table;
    }

    @Override
    public void setFields(List<String> fields) {
        this.fields.clear();
        this.fields.addAll(fields);
        String fieldsStr = BanyanTypeUtil.joinNWrap(this.fields, ",", "\"", "\"");
        String symbols = BanyanTypeUtil.repeat("?", fields.size(), ",");
        sql = String.format(GENERIC_UPSERT_SQL, getTable(), fieldsStr, symbols);
    }
}
