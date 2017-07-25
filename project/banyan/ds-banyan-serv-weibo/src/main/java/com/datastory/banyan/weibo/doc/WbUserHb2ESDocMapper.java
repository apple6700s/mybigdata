package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;

import java.util.List;


/**
 * com.datastory.banyan.weibo.doc.WbUserHb2ESDocMapper
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WbUserHb2ESDocMapper extends ParamsDocMapper {
    public WbUserHb2ESDocMapper(Params in) {
        super(in);
    }

    public static final String[] shortFields = {
            "verified_type", "vtype", "user_type",
            "gender", "fans_level"
    };

    public static final String[] mappingFields = {
            "fans",
            "desc",
            "name",
            "update_date",
            "publish_date",
            "fans_level",
            "province",
            "city",
            "activeness",
            "wb_cnt",
            "fav_cnt",
            "fans_cnt",
            "follow_cnt",
            "vtype",
            "verified_type",
            "user_type",
            "birthdate",
            "birthyear",
            "constellation",
            "follow_list"
    };

    public static final String[] yzListFields = {
            "group", "meta_group",
            "company", "school",
            "keywords", "topics",
            "footprints", "sources",
    };

    @Override
    public Params map() {
        if (in == null || in.size() <= 1)
            return null;

        Params p = new Params();
        p.put("id", getString("uid"));

        BanyanTypeUtil.safePut(p, "gender", getString("gender"));

        for (String field : mappingFields) {
            String v = getString(field);
            BanyanTypeUtil.safePut(p, field, v);
        }

        for (String field : yzListFields) {
            String v = getString(field);
            if (!StringUtil.isNullOrEmpty(v)) {
                p.put(field, BanyanTypeUtil.yzStr2List(v));
            }
        }

        if (p.get("meta_group") != null) {
            List<String> list = p.getList("meta_group", String.class);
            list.remove(0);
            p.put("meta_group", list);
        }

        for (String field : shortFields) {
            String v = getString(field);
            if (!StringUtil.isNullOrEmpty(v)) {
                p.put(field, BanyanTypeUtil.parseShort(v));
            }
        }

        return p;
    }
}
