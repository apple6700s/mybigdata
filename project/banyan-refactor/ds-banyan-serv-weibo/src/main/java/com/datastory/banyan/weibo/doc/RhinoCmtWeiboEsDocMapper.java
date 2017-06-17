package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Get;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.RhinoCmtWeiboEsDocMapper
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class RhinoCmtWeiboEsDocMapper extends ParamsDocMapper {
    public static final String TBL_WBCNT = Tables.table(Tables.PH_WBCNT_TBL);
    public static final String TBL_WBUSER = Tables.table(Tables.PH_WBUSER_TBL);

    public RhinoCmtWeiboEsDocMapper(Params in) {
        super(in);
    }

    static RFieldGetter cntReader = new RFieldGetter(TBL_WBCNT);
    public static RhinoCmtWeiboEsDocMapper build(String mid) throws IOException {
        String pk = BanyanTypeUtil.wbcontentPK(mid);
        Params hbDoc = new Params(cntReader.get(pk));
        return new RhinoCmtWeiboEsDocMapper(hbDoc);
    }

    public static final Map<String, String> RENAME_MAPPING = BanyanTypeUtil.strArr2strMap(new String[]{
            "mid", "id",
            "uid", "uid",
            "content", "content",
            "sentiment", "sentiment",
            "src_mid", "retweet_id",
            "src_content", "retweet_content",
            "self_content", "self_content",
            "msg_type", "msg_type",
            "rt_mid", "pid",
    });

    public static final String[] SHORT_FIELDS = {
            "sentiment", "msg_type",
    };

    @Override
    public Params map() {
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, RENAME_MAPPING);
        BanyanTypeUtil.putShortNotNull(out, in, SHORT_FIELDS);

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (in.get("publish_date") != null) {
                    try {
                        Date pDate = DateUtils.parse(in.getString("publish_date"), DateUtils.DFT_TIMEFORMAT);
                        long postTime = pDate.getTime();
                        out.put("post_time", postTime);
                        out.put("post_time_hour", DateUtils.getHourStr(pDate));
                        out.put("post_time_date", DateUtils.getDateStr(pDate));
                    } catch (Exception ignore) {
                    }
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (out.getString("content") != null) {
                    out.put("content_length", out.getString("content").length());
                    if (out.getString("self_content") == null) {
                        out.put("self_content", SelfContentExtractor.extract(out.getString("content")));
                    }

                    if (out.getString("self_content") != null)
                        out.put("self_content_length", out.getString("self_content").length());


                    if (out.getString("retweet_content") != null)
                        out.put("retweet_content_length", out.getString("retweet_content").length());
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                Integer userType = out.getInt("user_type");
                short isRobot = 0;
                if (userType != null)
                    isRobot = (short) WbUserAnalyzer.userType2isRobot(userType);
                out.put("is_robot", isRobot);
                return null;
            }
        });

        getRetweetContent(out);

        return out;
    }

    static RFieldGetter userReader = new RFieldGetter(TBL_WBUSER);

    public RhinoCmtWeiboEsDocMapper fillUser() throws IOException {
        String cntUid = in.getString("uid");
        String pk = BanyanTypeUtil.wbuserPK(cntUid);
        Get userGet = new Get(pk.getBytes());
        userGet.addColumn("r".getBytes(), "user_type".getBytes());
        Map<String, String> cntUser = userReader.get(userGet);
        if (cntUser != null) {
            cntUser.remove("pk");
            add(cntUser);
        } else {
            LOG.error("Null weibo user_type in user : " + cntUid);
            add(new StrParams("user_type", "-1"));
        }

        return this;
    }



    public static Params getRetweetContent(Params out) {
        if (out == null || out.size() <= 1)
            return null;

        String srcMid = out.getString("retweet_id");
        if (StringUtil.isNullOrEmpty(srcMid) || out.get("retweet_content") != null)
            return out;
        String pk = BanyanTypeUtil.wbcontentPK(srcMid);
        RFieldGetter fieldGetter = new RFieldGetter(TBL_WBUSER, "content");
        try {
            Map<String, String> res = fieldGetter.get(pk);
            if (res != null) {
                String content = res.get("content");
                if (!StringUtil.isNullOrEmpty(content)) {
                    out.put("retweet_content", content);
                    out.put("retweet_content_length", content.length());
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return out;
    }
}
