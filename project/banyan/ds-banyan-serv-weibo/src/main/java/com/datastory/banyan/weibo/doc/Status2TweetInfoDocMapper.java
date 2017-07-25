package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.doc.DocMapper;
import com.datastory.banyan.doc.ReflectDocMapper;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzTweetInfo;
import weibo4j.model.Status;
import weibo4j.model.User;

import java.lang.reflect.Field;

/**
 * com.datastory.banyan.weibo.doc.Status2TweetInfoDocMapper
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class Status2TweetInfoDocMapper extends ReflectDocMapper<Status> {
    public Status2TweetInfoDocMapper(Status in) {
        super(in);
    }

    @Override
    public UserAnalyzTweetInfo map() {
        Status status = this.in;
        UserAnalyzTweetInfo tweet = new UserAnalyzTweetInfo();
        tweet.setAttitudeCount(status.getAttitudesCount());
        tweet.setCommentCount(status.getCommentsCount());
        String createdAt = DateUtils.getTimeStr(status.getCreatedAt());

        tweet.setCreateAt(createdAt);
        tweet.setLatitude(status.getLatitude());
        tweet.setLongtitude(status.getLongitude());
        tweet.setRepostCount(status.getRepostsCount());
        tweet.setText(status.getText());

        if (status.getSource() != null)
            tweet.setSource(status.getSource().getName());

        if (status.getRetweetedStatus() != null) {
            Status retweet = status.getRetweetedStatus();
            tweet.setRetweetCommentCount(retweet.getCommentsCount());
            tweet.setRetweetRepostCount(retweet.getRepostsCount());
            tweet.setRetweetText(retweet.getText());
            if (retweet.getUser() != null) {
                User ru = retweet.getUser();
                tweet.setRetweetBiFollowersCount(ru.getBiFollowersCount());
                tweet.setRetweetFansCount(ru.getFollowersCount());
                tweet.setRetweetFavouritesCount(ru.getFavouritesCount());
                tweet.setRetweetFriendsCount(ru.getFriendsCount());
                tweet.setRetweetHead(ru.getAvatarLarge());
                tweet.setRetweetName(ru.getName());
                tweet.setRetweetUid(ru.getId());
                tweet.setRetweetUrl(ru.getUrl());
                tweet.setRetweetUserDomain(ru.getUserDomain());
                tweet.setRetweetVerifiedType(ru.getVerifiedType());
                tweet.setRetweetWeiHao(ru.getWeihao());
                tweet.setRetweetStatusesCount(ru.getStatusesCount());
            }
        }

        return tweet;
    }
}
