package com.datastory.banyan.weibo.doc.scavenger;

import com.datastory.banyan.doc.ResultSetDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzTweetInfo;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.scavenger.ScavengerTweetRSDocMapper
 *
 * @author lhfcws
 * @since 2016/10/25
 */
@Deprecated
public class ScavengerTweetRSDocMapper extends ResultSetDocMapper {
    public ScavengerTweetRSDocMapper(ResultSet rs) {
        super(rs);
    }

    /**
     * Hbase params -> UserAnalyzBasicInfo
     */
    private static final String[] field_UserAnalyzTweetInfo = {
            "mid", "wid",
            "content", "text",
            "create_date", "createAt",
            "reposts_cnt", "repostCount",
            "comments_cnt", "commentCount",
            "attitudes_cnt", "attitudeCount",
            "source", "source"
    };

    private UserAnalyzTweetInfo params2UserAnalyzTweetInfo(ResultSet rs) throws Exception {
        UserAnalyzTweetInfo userAnalyzTweetInfo = new UserAnalyzTweetInfo();
        Class<UserAnalyzTweetInfo> klass = UserAnalyzTweetInfo.class;
        StrParams fields = BanyanTypeUtil.strArr2strMap(field_UserAnalyzTweetInfo);

        for (Map.Entry<String, String> e : fields.entrySet()) {
            String attr = e.getValue();
            try {
                Field field = klass.getDeclaredField(attr);
                if (field == null) continue;
                field.setAccessible(true);
                if (field.getDeclaringClass().equals(Integer.class)) {
                    try {
                        Integer i = BanyanTypeUtil.parseInt(rs.getString(e.getKey()));
                        field.set(userAnalyzTweetInfo, i);
                    } catch (Exception ignore) {
                    }
                } else if (field.getDeclaringClass().equals(String.class)) {
                    field.set(userAnalyzTweetInfo, rs.getString(e.getKey()));
                }
            } catch (NoSuchFieldException ignore) {}
        }

        return userAnalyzTweetInfo;
    }

    @Override
    public UserAnalyzTweetInfo map() {
        try {
            return params2UserAnalyzTweetInfo(this.rs);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        } finally {
            close();
        }
    }
}
