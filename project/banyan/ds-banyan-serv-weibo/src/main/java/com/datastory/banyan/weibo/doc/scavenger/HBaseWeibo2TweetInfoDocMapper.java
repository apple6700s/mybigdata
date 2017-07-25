package com.datastory.banyan.weibo.doc.scavenger;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzTweetInfo;

import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.scavenger.HBaseWeibo2TweetInfoDocMapper
 *
 * @author lhfcws
 * @since 16/11/15
 */

public class HBaseWeibo2TweetInfoDocMapper extends ParamsDocMapper {
    public HBaseWeibo2TweetInfoDocMapper(Params p) {
        super(p);
    }

    public HBaseWeibo2TweetInfoDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    @Override
    public UserAnalyzTweetInfo map() {
        UserAnalyzTweetInfo tweet = new UserAnalyzTweetInfo();
        tweet.setWid(getString("mid"));
        tweet.setAttitudeCount(getInt("attitudes_cnt"));
        tweet.setCommentCount(getInt("comments_cnt"));
        tweet.setRepostCount(getInt("reposts_cnt"));
        tweet.setCreateAt(getString("publish_date"));

        String content = getString("content");
        if (!StringUtil.isNullOrEmpty(content)) {
            String srcContent = SelfContentExtractor.extractSrcContent(content);
            int srcLen = BanyanTypeUtil.len(srcContent);
            tweet.setRetweetText(srcContent);
            tweet.setText(content.substring(0, content.length() - srcLen));
        }
        tweet.setSource(getString("source"));
        return tweet;
    }
}
