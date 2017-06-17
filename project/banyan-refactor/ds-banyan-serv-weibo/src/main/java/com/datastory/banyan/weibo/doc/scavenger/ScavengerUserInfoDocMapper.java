package com.datastory.banyan.weibo.doc.scavenger;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzBasicInfo;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.scavenger.ScavengerUserInfoDocMapper
 *
 * @author lhfcws
 * @since 2016/10/25
 */
@Deprecated
public class ScavengerUserInfoDocMapper extends ParamsDocMapper {

    public ScavengerUserInfoDocMapper(Params p) {
        super(p);
    }

    /**
     * Hbase params -> UserAnalyzBasicInfo
     */
    private static final String[] field_UserAnalyzBasicInfo = {
            "uid", "uid",
            "name", "name",
            "name", "nickname",
            "head_url", "head",
            "url", "url",
            "verified_type", "verifiedType",
            "domain", "userDomain",
            "weihao", "weiHao",
            "follow_cnt", "friendsCount",
            "bi_follow_cnt", "biFollowersCount",
            "fans_cnt", "followerCount",
            "fav_cnt", "favouritesCount",
            "wb_cnt", "statusesCount",
            "create_date", "registerTime",
            "last_tweet_date", "lastTweetTime"
    };
    private static final StrParams fields = BanyanTypeUtil.strArr2strMap(field_UserAnalyzBasicInfo);
//    private static final HashSet<String> valueSet = new HashSet<>(fields.values());

    private UserAnalyzBasicInfo params2UserAnalyzBasicInfo(Params user) throws Exception {
        Class<UserAnalyzBasicInfo> klass = UserAnalyzBasicInfo.class;
        UserAnalyzBasicInfo userAnalyzBasicInfo = new UserAnalyzBasicInfo();

        for (Map.Entry<String, String> e : fields.entrySet()) {
            try {
                String attr = e.getValue();
                String value = user.getString(e.getKey());
                Field field = klass.getDeclaredField(attr);
                if (field == null) continue;
                field.setAccessible(true);
                if (field.getType().equals(Integer.class)) {
                    try {
                        Integer i = 0;
                        if (value != null)
                            i = BanyanTypeUtil.parseInt(value);
                        field.set(userAnalyzBasicInfo, i);
                    } catch (Exception ignore) {
                        field.set(userAnalyzBasicInfo, 0);
                    }
                } else if (field.getType().equals(String.class)) {
                    if (value == null) continue;
                    field.set(userAnalyzBasicInfo, value);
                }
            } catch (NoSuchFieldException ignore) {
            }
        }

        userAnalyzBasicInfo.setTags(BanyanTypeUtil.yzStr2List(
                user.getString("tags")
        ));

        return userAnalyzBasicInfo;
    }

    @Override
    public UserAnalyzBasicInfo map() {
        try {
            return params2UserAnalyzBasicInfo(this.in);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
