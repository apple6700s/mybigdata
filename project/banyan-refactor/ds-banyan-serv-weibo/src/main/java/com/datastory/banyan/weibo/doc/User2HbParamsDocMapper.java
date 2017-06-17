package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.analyz.GenderTranslator;
import com.datastory.banyan.doc.ReflectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;
import weibo4j.model.User;

import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.User2HbParamsDocMapper
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class User2HbParamsDocMapper extends ReflectDocMapper<User> {

    public User2HbParamsDocMapper(User user) {
        super(user);
    }

    private static final String[] mappingFields_ = {
            "id", "uid",
            "name", "name",
            "province", "province",
            "city", "city",
            "url", "blog_url",
            "description", "desc",
            "followersCount", "fans_cnt",
            "friendsCount", "follow_cnt",
            "statusesCount", "wb_cnt",
            "favouritesCount", "fav_cnt",
            "verifiedType", "verified_type",
            "verifiedReason", "verified_reason",
            "weihao", "weihao",
            "userDomain", "domain",
            "biFollowersCount", "bi_follow_cnt",
            "profileImageUrl", "head_url",
            "location", "location",
    };
    private static final Map<String, String> mappingFields = BanyanTypeUtil.strArr2strMap(mappingFields_);

    @Override
    public Params map() {
        User user = this.in;
        if (user == null)
            return null;
        Params p = new Params();

        // direct rename mapping
        for (Map.Entry<String, String> e : mappingFields.entrySet()) {
            BanyanTypeUtil.safePut(p, e.getValue(), getString(e.getKey()));
        }

        p.put("pk", BanyanTypeUtil.wbuserPK(p.getString("uid")));

        p.put("gender", GenderTranslator.translate(user.getGender()));

        // publish_date && update_date
        Date createDate = user.getCreatedAt();
        p.put("publish_date", DateUtils.getTimeStr(createDate));
        p.put("update_date", DateUtils.getCurrentTimeStr());

        // url
        p.put("url", "http://weibo.com/u/" + user.getId());

        return p;
    }
}
