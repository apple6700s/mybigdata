package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.weibo.analyz.MsgTypeAnalyzer;
import com.datastory.banyan.doc.ReflectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.datastory.banyan.weibo.analyz.WbContentAnalyzer;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import weibo4j.model.Source;
import weibo4j.model.Status;

import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.Status2HbParamsDocMapper
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class Status2HbParamsDocMapper extends ReflectDocMapper<Status> {
    public Status2HbParamsDocMapper(Status status) {
        super(status);
    }

    private static final String[] mappingFields_ = {
            "mid", "mid",
            "uid", "uid",
            "text", "content",
            "geo", "geo",
            "repostsCount", "reposts_cnt",
            "commentsCount", "comments_cnt",
            "attitudesCount", "attitudes_cnt",
            "feature", "feature",
            "annotations", "annotations",
    };
    private static final Map<String, String> mappingFields = BanyanTypeUtil.strArr2strMap(mappingFields_);

    public Params simpleMap() {
        Status status = this.in;
        if (status == null)
            return null;

        Params p = new Params();

        // direct rename mapping
        for (Map.Entry<String, String> e : mappingFields.entrySet()) {
            String v = getString(e.getKey());
            BanyanTypeUtil.safePut(p, e.getValue(), v);
        }

        p.put("pk", BanyanTypeUtil.wbcontentPK(p.getString("mid")));

        // publish_date && update_date
        Date createDate = status.getCreatedAt();
        p.put("publish_date", DateUtils.getTimeStr(createDate));
        p.put("update_date", DateUtils.getCurrentTimeStr());

        // source
        Source source = status.getSource();
        if (source != null) {
            String sourceStr = source.getName() + "$" + source.getRelationship() + "$" + source.getUrl();
            BanyanTypeUtil.safePut(p, "source", sourceStr);
        }

        // picurl
        if (status.getPicUrls() != null)
            BanyanTypeUtil.safePut(p, "pic_urls", GsonSerializer.serialize(status.getPicUrls()));
        if (status.getOriginalPic() != null)
            BanyanTypeUtil.safePut(p, "original_pic", status.getOriginalPic());

        // getRetweetedStatus()其实就是src微博
        if (status.getRetweetedStatus() != null) {
            Status rtStatus = status.getRetweetedStatus();
            BanyanTypeUtil.safePut(p, "src_mid", rtStatus.getMid());
            BanyanTypeUtil.safePut(p, "rt_mid", status.getPid());
        }

        // msgtype
        BanyanTypeUtil.safePut(p, "self_content", SelfContentExtractor.extract(p.getString("content")));

        BanyanTypeUtil.safePut(p, "msg_type", MsgTypeAnalyzer.analyz(
                p.getString("src_mid"),
                p.getString("rt_mid"),
                p.getString("self_content"),
                p.getString("content")
        ));

        return p;
    }

    @Override
    public Params map() {
        Status status = this.in;
        if (status == null)
            return null;

        Params p = simpleMap();
        p = WbContentAnalyzer.getInstance().analyz(p);

        return p;
    }
}
