package com.datastory.banyan.weibo.doc.scavenger;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzBasicInfo;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzTweetInfo;

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * com.datatub.rhino.doc.scavenger.HBaseParams2UserInfoDocMapper
 *
 * @author lhfcws
 * @since 16/11/15
 */

public class HBaseParams2UserInfoDocMapper extends ParamsDocMapper {
    public HBaseParams2UserInfoDocMapper(Map<String, ? extends Object> p) {
        super(new Params(p));
    }

    public HBaseParams2UserInfoDocMapper(Params p) {
        super(p);
    }

    @Override
    public UserAnalyzBasicInfo map() {
        if (in== null || in.isEmpty())
            return null;

        UserAnalyzBasicInfo basicInfo = new UserAnalyzBasicInfo();
        basicInfo.setUid(getString("uid"));
        basicInfo.setName(getString("name"));
        basicInfo.setNickname(getString("name"));
        basicInfo.setVerifiedType(BanyanTypeUtil.parseInt(getString("verified_type")));
        basicInfo.setLastTweetTime(getString("last_tweet_time"));
        basicInfo.setHead(getString("head_url"));
        basicInfo.setUrl(getString("url"));
        basicInfo.setWeiHao(getString("weihao"));
        basicInfo.setUserDomain(getString("domain"));
        basicInfo.setStatusesCount(getInt("wb_cnt"));
        basicInfo.setFavouritesCount(getInt("fav_cnt"));
        basicInfo.setFollowerCount(getInt("fans_cnt"));
        basicInfo.setFriendsCount(getInt("follow_cnt"));
        basicInfo.setBiFollowersCount(getInt("bi_follow_cnt"));
        basicInfo.setRegisterTime(getString("create_date"));

        if (this.in.containsKey("_tweets")) {
            try {
                List<UserAnalyzTweetInfo> tweetInfoList = new LinkedList<>();
                List<Params> list = this.in.getList("_tweets", Params.class);

                for (Params tweet : list) {
                    UserAnalyzTweetInfo tweetInfo = new HBaseWeibo2TweetInfoDocMapper(tweet).map();
                    tweetInfoList.add(tweetInfo);
                }

                basicInfo.setTweets(tweetInfoList);
            } catch (Throwable ignore) {}
        }

        return basicInfo;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        StrParams p = new StrParams("uid", "1");
        Params p1 = new Params("uid", "2");
        HBaseParams2UserInfoDocMapper mapper = new HBaseParams2UserInfoDocMapper(p1);
        System.out.println(mapper);
        System.out.println("[PROGRAM] Program exited.");
    }
}
