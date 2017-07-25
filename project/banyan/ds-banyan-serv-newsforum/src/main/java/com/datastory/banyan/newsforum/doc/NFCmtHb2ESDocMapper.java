package com.datastory.banyan.newsforum.doc;

import com.datastory.banyan.analyz.HTMLTrimmer;
import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.net.URL;

/**
 * com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NFCmtHb2ESDocMapper extends ParamsDocMapper {
    public NFCmtHb2ESDocMapper(Params in) {
        super(in);
    }

    public static final String[] shortFields = {
            "cat_id", "sentiment", "is_ad", "is_robot",
    };

    public static final String[] intFields = {
            "review_cnt", "view_cnt", "like_cnt", "dislike_cnt"
    };

    public static final String[] mappingFields = {
            "title", "content", "author", "url",
            "source", "publish_date", "update_date",
            "fingerprint", "parent_post_id", "taskId",
            "review_cnt", "view_cnt", "like_cnt", "dislike_cnt"
    };

    @Override
    public Params map() {
        if (in == null || in.size() <= 1)
            return null;

        Params p = new Params();
        BanyanTypeUtil.putAllNotNull(p, in, mappingFields);
        BanyanTypeUtil.safePut(p, "domain", getDomain(p.getString("url")));

        HTMLTrimmer.trim(p, "content");
        HTMLTrimmer.trim(p, "author");
        HTMLTrimmer.trim(p, "title");

        p.put("id", getString("pk"));

        // list fields
        if (getString("keywords") != null) {
            p.put("keywords", BanyanTypeUtil.yzStr2List(getString("keywords")));
        }

        // content length
        p.put("content_len", BanyanTypeUtil.len(getString("content")));

        // publish_date
        PublishDateExtractor.extract(p, getString("publish_date"));

        for (String key : shortFields)
            p.put(key, BanyanTypeUtil.parseShortForce(getString(key)));

        for (String key : intFields)
            p.put(key, BanyanTypeUtil.parseIntForce(p.getString(key)));

        p.put("_parent", p.getString("parent_post_id"));

        return p;
    }

    /**
     * 获取url，使用URL类，取出并返回domain，即host。
     *
     * @param url
     * @return domain
     */
    private String getDomain(String url) {
        String domain = null;

        try {
            URL urlObject = new URL(url);
            domain = urlObject.getHost();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return domain;
    }
}
