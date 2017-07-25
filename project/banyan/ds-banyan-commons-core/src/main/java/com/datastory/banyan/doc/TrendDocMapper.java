package com.datastory.banyan.doc;

import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * com.datastory.banyan.doc.TrendDocMapper
 *
 * @author lhfcws
 * @since 2017/7/5
 */
public abstract class TrendDocMapper extends ParamsDocMapper {
    protected String source;
    protected String type;
    protected String parentIdField;
    protected String idField;

    public TrendDocMapper(Map<String, Object> doc, String source, String type, String parentIdField, String idField) {
        super(doc);
        this.source = source;
        this.type = type;
        this.parentIdField = parentIdField;
        this.idField = idField;
    }

    protected abstract String getData();

    @Override
    public Params map() {
        if (in == null || in.isEmpty())
            return null;

        Params p = new Params();
        String id = getString(idField);
        p.put("id", id);
        p.put("update_date", getString("update_date"));
        p.put("source", source);
        p.put("type", type);

        if (BanyanTypeUtil.valid(parentIdField)) {
            BanyanTypeUtil.safePut(p, "parent_id", getString(parentIdField));
        }

        String data = getData();
        if (data != null) {
            p.put("data", data);

            String pk = StringUtils.join(new String[]{
                    source, id, type
            }, "|");
            p.put("pk", pk);

            return p;
        } else
            return null;
    }
}
