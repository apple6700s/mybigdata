package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.weibo.analyz.RecentActivenessAnalyzer;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.WbUser2RhinoESDocMapper
 *
 * @author lhfcws
 * @since 2016/12/22
 */
public class WbUser2RhinoESDocMapper extends ParamsDocMapper {
    public WbUser2RhinoESDocMapper(Params in) {
        super(in);
    }

    static final String[] directM = {
            "update_date", "constellation", "verified_type", "birthyear", "birthdate",
            "province", "city", "activeness"
    };

    static final String[] listM = {
            "sources", "emojis", "school", "company", "meta_group", "topics",
    };

    static final String[] shortM = {
            "gender", "user_type", "vtype", "fans_level",
    };

    static final Map<String, String> renameM = BanyanTypeUtil.strArr2strMap(new String[]{
            "uid", "id",
            "name", "nickname",
            "desc", "description",
            "wb_cnt", "weibo_count",
            "fav_cnt", "favorite_count",
            "fans_cnt", "follower_count",
            "follow_cnt", "friend_count",
            "city_level", "city_type",
            "publish_date", "create_date",
            "create_date", "create_date",
    });

    @Override
    public Params map() {
        if (in == null || in.isEmpty())
            return null;
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, directM);
        BanyanTypeUtil.putAllNotNull(out, in, renameM);
        BanyanTypeUtil.putShortNotNull(out, in, shortM);
        BanyanTypeUtil.putYzListNotNull(out, in, listM);

        List<String> metagroup = out.getList("meta_group", String.class);
        if (!CollectionUtil.isEmpty(metagroup)) {
            String v = metagroup.get(0);
            try {
                Integer.valueOf(v);
                metagroup.remove(0);
                out.put("meta_group", metagroup);
            } catch (Exception ignore) {}
        }

        // followList
        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                Object f = in.get("follow_list");
                if (f != null) {
                    if (f instanceof List) {
                        if (!((List) f).isEmpty()) {
                            out.put("follower", f);
                        }
                    } else if (f instanceof String) {
                        String fStr = (String) f;
                        if (fStr.trim().length() > 0) {
                            List<String> followList = BanyanTypeUtil.yzStr2List(fStr);
                            out.put("follower", followList);
                        }
                    }
                }
                return null;
            }
        });

        if (out.get("activeness") == null)
            out.put("activeness", RecentActivenessAnalyzer.ACTIVENESS.NOT_ACTIVE.getValue() + "");

        return out;
    }
}
