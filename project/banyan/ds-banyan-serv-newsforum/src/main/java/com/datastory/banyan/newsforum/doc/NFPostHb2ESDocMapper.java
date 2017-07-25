package com.datastory.banyan.newsforum.doc;

import com.datastory.banyan.analyz.HTMLTrimmer;
import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.net.URL;

/**
 * com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NFPostHb2ESDocMapper extends ParamsDocMapper {
    public NFPostHb2ESDocMapper(Params in) {
        super(in);
    }

    public static final String[] shortFields = {
            "cat_id", "sentiment", "is_ad", "is_robot",
            "is_hot", "is_recom", "is_top", "is_digest",
    };

    public static final String[] intFields = {
            "review_cnt", "view_cnt", "like_cnt", "dislike_cnt"
    };

    public static final String[] fields = {
            "title", "content", "author", "url",
            "source", "publish_date", "update_date",
            "fingerprint", "site_id", "site_name",
            "all_content", "taskId",
            "review_cnt", "view_cnt", "like_cnt", "dislike_cnt",
            "introduction", "origin_label", "same_html_count"
    };

    @Override
    public Params map() {
        if (in == null || in.size() <= 1)
            return null;

        Params p = new Params();

        BanyanTypeUtil.putAllNotNull(p, in, fields);
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

        return p;
    }

    /**
     * 获取url，使用URL类，取出并返回domain，即host。
     *
     * @param url
     * @return domain
     */
    private static String getDomain(String url) {
        String domain = null;

        try {
            URL urlObject = new URL(url);
            domain = urlObject.getHost();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return domain;
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        System.out.println(getDomain("http://www.bj.chinanews.com/news/2016/1208/55598.html"));
        System.out.println("[PROGRAM] Program exited.");
    }
}
