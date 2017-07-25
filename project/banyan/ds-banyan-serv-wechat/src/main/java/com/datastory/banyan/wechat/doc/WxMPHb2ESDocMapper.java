package com.datastory.banyan.wechat.doc;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxMPHb2ESDocMapper extends ParamsDocMapper {
    public WxMPHb2ESDocMapper(Params in) {
        super(in);
    }

    public static final String[] mapping = {
            "name", "desc", "update_date",
            "fans_cnt", "verify_status",

            "wxid", "biz", "open_id"
    };

    public static final String AUTHOR_ID_FIELD = "biz";

    @Override
    public Params map() {
        if (in == null || in.size() <= 1)
            return null;

        Params esDoc = new Params();

        esDoc.put("id", getString("pk"));
//        esDoc.put("id", getString(AUTHOR_ID_FIELD));

        BanyanTypeUtil.putAllNotNull(esDoc, in, mapping);

        return esDoc;
    }
}
