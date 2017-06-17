package com.datastory.banyan.hbase;

import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.ErrorUtil;

import java.sql.SQLException;
import java.util.*;


/**
 * com.datastory.banyan.hbase.PhoenixWriter
 * 到时有空拆分PhoenixWriter
 *
 * @author lhfcws
 * @since 16/11/22
 */
@Deprecated
public abstract class BanyanPhoenixWriter extends PhoenixWriter {

    public BanyanPhoenixWriter() {
        super();
    }

    public BanyanPhoenixWriter(int cacheSize) {
        super(cacheSize);
    }

    protected void init() {
        try {
            setFields(this.driver.getAllColumnNames(getTable()));
        } catch (SQLException e) {
            ErrorUtil.error(LOG, e);
        }
    }

    public void setFields(List<String> fields) {
        this.fields.clear();
        this.fields.add("pk");
        if (fields.contains("update_date"))
            this.fields.add("update_date");
        if (fields.contains("publish_date"))
            this.fields.add("publish_date");
        else if (fields.contains("create_date"))
            this.fields.add("create_date");

        Set<String> tmpSet = new HashSet<>(this.fields);
        for (String field : fields) {
            if (!tmpSet.contains(field))
                this.fields.add(field);
        }
        String fieldsStr = BanyanTypeUtil.joinNWrap(this.fields, ",", "\"", "\"");
        String symbols = BanyanTypeUtil.repeat("?", fields.size(), ",");
        sql = String.format(GENERIC_UPSERT_SQL, getTable(), fieldsStr, symbols);
    }

    public void setFields(String[] fields) {
        setFields(Arrays.asList(fields));
    }
}
