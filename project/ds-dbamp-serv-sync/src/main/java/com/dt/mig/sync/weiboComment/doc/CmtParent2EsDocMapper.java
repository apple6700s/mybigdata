package com.dt.mig.sync.weiboComment.doc;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.extract.SelfContentExtractor;
import com.dt.mig.sync.hbase.RFieldGetter;
import com.dt.mig.sync.hbase.doc.ParamsDocMapper;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.DateUtils;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.weiboComment.doc.CmtParent2EsDocMapper
 *
 * @author lhfcws
 * @since 2017/4/5
 */
public class CmtParent2EsDocMapper extends ParamsDocMapper {
    protected Short isRobot = null;


    public CmtParent2EsDocMapper(Params in) {
        super(in);
    }

    public CmtParent2EsDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    public CmtParent2EsDocMapper setUserType(String userType) {
        if (StringUtils.isNotEmpty(userType)) {
            isRobot = ("2".equals(userType)) ? new Short("0") : new Short("1");
        }
        return this;
    }


    protected Map<String, String> cntMapping = BanyanTypeUtil.strArr2strMap(new String[]{"mid", "id", "msg_type", "msg_type", "uid", "uid", "rt_mid", "pid", "content", "content", "src_content", "retweet_content", "src_uid", "retweet_uid", "src_mid", "retweet_id", "self_content", "self_content", "sentiment", "sentiment", "mig_mention", "mig_mention"});

    @Override
    public Params map() {
        if (in == null) {
            System.out.println("[in] 参数等于空");
            return null;
        }
        try {
            final Params out = new Params();
            if (isRobot != null) {
                out.put("is_robot", isRobot);
            }
            BanyanTypeUtil.putAllNotNull(out, in, cntMapping);

            if (out.containsKey("msg_type")) {
                BanyanTypeUtil.safePut(out, "msg_type", BanyanTypeUtil.parseShort(out.getString("msg_type")));
            }
            if (out.containsKey("sentiment")) {
                BanyanTypeUtil.safePut(out, "sentiment", BanyanTypeUtil.parseShort(out.getString("sentiment")));
            }

//            this.function(new Function<Void, Void>() {
//                @Nullable
//                @Override
//                public Void apply(@Nullable Void aVoid) {

            if (out.containsKey("src_mid")) {
                System.out.println("src_mid:" + out.getString("src_mid"));
                List<String> fields = new ArrayList<>();

                if (!out.containsKey("src_uid")) {
                    fields.add("uid");
                }

                if (!out.containsKey("src_content")) {
                    fields.add("content");
                }
                System.out.println("fields:" + fields);

                if (!fields.isEmpty()) {
                    RFieldGetter getter = new RFieldGetter(MigSyncConsts.HBASE_WEIBO_CONTENT_TBL_NEW, fields);
                    try {
                        System.out.printf("开始获取src_mid的内容!" + out.getString("src_mid"));
                        Params src = getter.get(BanyanTypeUtil.wbcontentPK(out.getString("src_mid")));
                        if (src != null && !src.isEmpty()) {
                            out.put("src_uid", src.getString("uid"));
                            out.put("src_content", src.getString("content"));
                        }
                        System.out.printf("完成获取src_mid内容!" + out.getString("src_mid"));
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                }
            }

//                    return null;
//                }
//            });

//            this.function(new Function<Void, Void>() {
//                @Nullable
//                @Override
//                public Void apply(@Nullable Void aVoid) {
            if (out.containsKey("content") && out.getString("content") != null) {
                out.put("content_length", out.getString("content").length());
            }

            if (!out.containsKey("self_content") || out.getString("self_content") == null) {
                if (out.containsKey("content") && out.getString("content") != null) {
                    out.put("self_content", SelfContentExtractor.extract(out.getString("content")));
                }
            }

            if (out.containsKey("self_content") && out.getString("self_content") != null) {
                out.put("self_content_length", out.getString("self_content").length());
            }

            if (out.containsKey("retweet_content") && out.getString("retweet_content") != null) {
                out.put("retweet_content_length", out.getString("retweet_content").length());
            }

//                    return null;
//                }
//            });

//            this.function(new Function<Void, Void>() {
//                @Nullable
//                @Override
//                public Void apply(@Nullable Void aVoid) {
            if (in != null && in.containsKey("publish_date") && in.get("publish_date") != null) {
                try {
                    Date pDate = DateUtils.parse(in.getString("publish_date"), DateUtils.DFT_TIMEFORMAT);
                    long postTime = pDate.getTime();
                    out.put("post_time", postTime);
                    out.put("post_time_hour", DateUtils.getHourStr(pDate));
                    out.put("post_time_date", DateUtils.getDateStr(pDate));
                } catch (Exception ignore) {
                }
            }
//                    return null;
//                }
//            });

            // fixed for abel : mig mention 移至wbCommentSync里面做分析

            return out;
        } catch (Exception e) {
            System.out.println("[ERROR] :" + e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        String value = "{\"uid\"=\"5862147128\"," + " \"pic_urls\"=[\"http://wx2.sinaimg.cn/thumbnail/006oIYg8ly1fe19xrf5h1j31120ku7wh.jpg\",\"http://wx3.sinaimg.cn/thumbnail/006oIYg8ly1fe19ypgb5bj31120kub29.jpg\"," + "\"http://wx1.sinaimg.cn/thumbnail/006oIYg8ly1fe19y85x1kj31120ku4qp.jpg\",\"http://wx3.sinaimg.cn/thumbnail/006oIYg8ly1fe19xxp4xhj31120ku1kx.jpg\"], " + "\"msg_type\"=0," + "\"emoji\"=[\"doge\"]," + "\"sentiment\"=1, " + "\"fingerprint\"=\"3f3085f861fc7d2eb6d6bd814302ad9f\", " + "\"username\"=\"明星大侦探官微\"," + "\"self_content\"=\"#明星大侦探# @何炅 见@蘇有朋 秒变迷弟求合照，@撒贝宁 被忽视张大嘴求注意[doge] http://t.cn/R667Imw\", " + "\"original_pic\"=\"http://wx2.sinaimg.cn/large/006oIYg8ly1fe19xrf5h1j31120ku7wh.jpg\"," + "\"is_ad\"=0, " + "\"feature\"=0," + "\"mention\"=\"何炅|蘇有朋|撒贝宁\"," + "\"geo\"=null, " + "\"keywords\"=\"何炅|合照|张大嘴求\", " + "\"publish_date\"=\"20170327130005\"," + "\"attitudes_cnt\"=768, " + "\"short_link\"=\"http://t.cn/R667Imw\"," + "\"url\"=\"http://weibo.com/5862147128/EBICz42tT\", " + "\"content\"=\"#明星大侦探# @何炅 见@蘇有朋 秒变迷弟求合照，@撒贝宁 被忽视张大嘴求注意[doge] http://t.cn/R667Imw\"," + "\"topics\"=\"明星大侦探\", " + "\"source\"=[\"微博 weibo.com\"], " + "\"update_date\"=\"20170331054829\"," + "\"reposts_cnt\"=8, " + "\"comments_cnt\"=29, " + "\"mid\"=\"4089896630962853\"," + "\"pk\"=\"d1b4089896630962853\"" + "}";
        Gson gson = new Gson();
        Params p = gson.fromJson(value, new TypeToken<Params>() {
        }.getType());
        Params parent = new CmtParent2EsDocMapper(p).map();


        System.out.println(123);
    }
}
