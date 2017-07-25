package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.Pair;

import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.hornbill.algo.astrotufing.classifier.util.AnalyzUtil;
import com.yeezhao.hornbill.algo.astrotufing.util.CoreConsts;
import com.yeezhao.hornbill.algo.astrotufing.util.FeatureUtil;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzBasicInfo;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzTweetInfo;
import com.yeezhao.hornbill.analyz.common.train.AccumulatorMap;
import com.datastory.banyan.weibo.doc.scavenger.HBaseWeibo2TweetInfoDocMapper;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 通过用户基本信息（及其所发的微博），使用规则过滤方法过滤僵尸
 * <p>
 * Author: abekwok
 * Create: 10/9/13
 * <p>
 * 修改内容：
 * <p>
 * Modify: mouhao, Lhfcws
 * Modified at: 2017-01-06
 */
public class AstroturfingClassifier {
    static Log LOG = LogFactory.getLog(AstroturfingClassifier.class);
    private static final Long SIX_MONTH_TIME = 6 * 30 * 24 * 3600 * 1000L;

    /**
     * 总体判断，通过微博用户的账号信息和微博信息进行判断
     *
     * @param userAnalyzBasicInfo
     * @return 1: 正常用户
     * 0: 不确定
     * -1：僵尸／水军用户
     */
    public static int classify(UserAnalyzBasicInfo userAnalyzBasicInfo) {
        return _classify(userAnalyzBasicInfo).getFirst();
    }

    public static Pair<Integer, AccumulatorMap> _classify(UserAnalyzBasicInfo userAnalyzBasicInfo) {

        try {
            CoreConsts.VerifiedType vtype = CoreConsts.VerifiedType.fromOriginType(CoreConsts.DATA_SRC.sn, userAnalyzBasicInfo.getVerifiedType());
            if (vtype.equals(CoreConsts.VerifiedType.BLUE) || vtype.equals(CoreConsts.VerifiedType.YELLOW)) {
                return new Pair<>(1, new AccumulatorMap());
            }
        } catch (NullPointerException ne) {

        }

        if (userAnalyzBasicInfo.getTweets() == null || userAnalyzBasicInfo.getTweets().size() <= 5) {
            return _classifySimple(userAnalyzBasicInfo);
        }

        Pair<Integer, AccumulatorMap> base = null;
        try {
            base = basicAnalyz(userAnalyzBasicInfo);
        } catch (NullPointerException np) {
            return new Pair<>(0, new AccumulatorMap());
        }

        Pair<Integer, AccumulatorMap> advance = advancedAnalyz(userAnalyzBasicInfo.getTweets());

        int baseRate = base.getFirst();
        int advancedRate = advance.getFirst();

        AccumulatorMap resStatMap = AccumulatorMap.add(base.getSecond(), advance.getSecond());
        resStatMap.put("base.rate", baseRate);
        resStatMap.put("advance.rate", advancedRate);

        LOG.debug(String.format("SimpleUserFilterOpRates[%s]: total(%s),basis(%s),advanced(%s)", userAnalyzBasicInfo.getUid(), baseRate + advancedRate, baseRate, advancedRate));


        return new Pair<>(advanceJudge(baseRate, advancedRate), resStatMap);
    }

    /**
     * 仅使用基本信息做分析
     * -1: 僵尸
     * 0:不确定
     * 1:正常
     *
     * @return
     */
    public static int classifySimple(UserAnalyzBasicInfo userAnalyzBasicInfo) {
        return _classifySimple(userAnalyzBasicInfo).getFirst();
    }

    public static Pair<Integer, AccumulatorMap> _classifySimple(UserAnalyzBasicInfo userAnalyzBasicInfo) {
        try {
            CoreConsts.VerifiedType vtype = CoreConsts.VerifiedType.fromOriginType(CoreConsts.DATA_SRC.sn, userAnalyzBasicInfo.getVerifiedType());
            if (vtype.equals(CoreConsts.VerifiedType.BLUE) || vtype.equals(CoreConsts.VerifiedType.YELLOW)) {
                return new Pair<>(1, new AccumulatorMap());
            }
        } catch (NullPointerException ignore) {
        }

        Pair<Integer, AccumulatorMap> base = null;

        try {
            base = basicAnalyz(userAnalyzBasicInfo);
        } catch (NullPointerException np) {
            return new Pair<>(0, new AccumulatorMap());
        }

        return new Pair<>(simpleJudge(base.getFirst()), base.getSecond());
    }

    /**
     * 通过用户基础分数判断是否为水军
     *
     * @param simple
     * @return
     */
    public static Integer simpleJudge(Integer simple) {
        if (simple < 4) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * 得分计算公式
     *
     * @param simple:  通过微博用户信息得到的水军分数
     * @param advance: 通过微博用户微博数据得到的水军分数
     * @return
     */
    public static Integer advanceJudge(Integer simple, Integer advance) {
        Integer totalRate = simple + advance;
        if (totalRate > 0) {
            if (totalRate > 10
                    || simple > 7
                    || advance > 3
                    || (simple > 0 && advance > 0)
                    || (simple > 2 && advance >= 0)
                    || (simple >= 0 && advance > 2)) {
                return -1;
            } else {
                return 0;
            }
        } else if (totalRate == 0) {
            if (simple > 3 || advance > 3) {
                return 0;
            } else {
                return 1;
            }
        } else {
            if (advance > 6 || simple > 6) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    /**
     * @param userAnalyzBasicInfo
     * @return Pair<得分,判断值>
     * 得分: 基础判断得分
     * 判断值:
     * 1:   正常用户
     * 0:   无法判断
     * -1:  僵尸
     * 2:   黄v和蓝v用户
     * @throws NullPointerException
     */
    public static Pair<Integer, Integer> classifySimpleWithStatus(UserAnalyzBasicInfo userAnalyzBasicInfo) throws NullPointerException {
        try {
            CoreConsts.VerifiedType vtype = CoreConsts.VerifiedType.fromOriginType(CoreConsts.DATA_SRC.sn, userAnalyzBasicInfo.getVerifiedType());
            if (vtype.equals(CoreConsts.VerifiedType.BLUE) || vtype.equals(CoreConsts.VerifiedType.YELLOW)) {
                return new Pair<>(0, 2);
            }
        } catch (NullPointerException ne) {

        }

        Pair<Integer, AccumulatorMap> base = null;
        try {
            base = basicAnalyz(userAnalyzBasicInfo);
        } catch (NullPointerException np) {
            LOG.error(np);
            return new Pair(0, 0);
        }

        return new Pair<>(base.getFirst(), simpleJudge(base.getFirst()));
    }

    /**
     * 传入同一用户的微博数据，返回基于微博信息的水军分值
     *
     * @param tweets
     * @return
     */
    public static int classifyAdvance(Iterable<UserAnalyzTweetInfo> tweets) {
        if (tweets == null) {
            return 0;
        }

        return advancedAnalyz(tweets).getFirst();
    }

    /**
     * 传入同一用户的微博数据，返回基于微博信息的水军分值
     *
     * @param tweets
     * @return
     */
    public static int classifyAdvanceBanyan(Iterable<? extends Object> tweets) {
        if (tweets == null) {
            return 0;
        }

        return advancedAnalyzBanyan(tweets).getFirst();
    }


    /**
     * 根据基本信息判断僵尸系数。
     * 以0为界限：正值越大，表示僵尸系数越高；负值越大，表示真实系数越大
     *
     * @param userAnalyzBasicInfo
     * @return
     */
    private static Pair<Integer, AccumulatorMap> basicAnalyz(UserAnalyzBasicInfo userAnalyzBasicInfo) throws NullPointerException {

        Integer friendsCount;
        Integer followersCount;
        Integer tweetsCount;
        Integer biFollowerCount;
        Integer favouriteCount;
        String nickname;

        friendsCount = userAnalyzBasicInfo.getFriendsCount();
        followersCount = userAnalyzBasicInfo.getFollowerCount();
        tweetsCount = userAnalyzBasicInfo.getStatusesCount();
        biFollowerCount = userAnalyzBasicInfo.getBiFollowersCount();
        favouriteCount = userAnalyzBasicInfo.getFavouritesCount();
        if (!StringUtil.isNullOrEmpty(userAnalyzBasicInfo.getNickname())) {
            nickname = userAnalyzBasicInfo.getNickname();
        } else {
            nickname = userAnalyzBasicInfo.getName();
        }

        // 空值判断
        if (friendsCount == null || followersCount == null || tweetsCount == null || biFollowerCount == null || favouriteCount == null || nickname == null) {
            throw new NullPointerException("基础判断数值为空");
        }

        String url = userAnalyzBasicInfo.getUrl();
        String userDomain = userAnalyzBasicInfo.getUserDomain();
        String weihao = userAnalyzBasicInfo.getWeiHao();

        boolean hasUrlDomainOrWeihao = (url != null && !url.isEmpty()) || (userDomain != null && !userDomain.isEmpty()) || (weihao != null && !weihao.isEmpty());
        return _basicAnalyz(nickname, userAnalyzBasicInfo.getLastTweetTime(), userAnalyzBasicInfo.getHead(), hasUrlDomainOrWeihao, tweetsCount, favouriteCount, biFollowerCount, followersCount, friendsCount);
    }

    /**
     * 根据基本信息判断僵尸系数。
     * 以0为界限：正值越大，表示僵尸系数越高；负值越大，表示真实系数越大
     *
     * @param nickname
     * @param lastTweetTime
     * @param head
     * @param hasUrlDomainOrWeihao
     * @param tweetsCount
     * @param favouritesCount
     * @param biFollowersCount
     * @param followersCount
     * @param friendsCount
     * @return
     */
    private static Pair<Integer, AccumulatorMap> _basicAnalyz(
            String nickname,
            String lastTweetTime,
            String head,
            boolean hasUrlDomainOrWeihao,
            int tweetsCount,
            int favouritesCount,
            int biFollowersCount,
            int followersCount,
            int friendsCount) {
        int rate = 0;
        int minRelatedCount = Math.min(followersCount, friendsCount);
        float followersCountDvdFriendsCount = ((float) followersCount) / ((float) friendsCount);
        AccumulatorMap map = new AccumulatorMap();

        if (hasUrlDomainOrWeihao) {
            LOG.debug(" -2 hasUrlDomainOrWeihao");
            map.insert("basic.hasUrlDomainOrWeihao");
            rate -= 2;
        }
        if (favouritesCount > (tweetsCount + 1000) / 1000) {
            LOG.debug(" -2 favouritesCount>(tweetsCount+1000)/1000");
            map.insert("basic.favouritesCount>(tweetsCount+1000)/1000");
            rate -= 2;
        }
        if (favouritesCount > (tweetsCount + 1000) / 200) {
            LOG.debug(" -2 favouritesCount>(tweetsCount+1000)/200");
            map.insert("basic.favouritesCount>(tweetsCount+1000)/200");
            rate -= 2;
        }
        if (followersCountDvdFriendsCount > 2 && biFollowersCount > minRelatedCount / 2) {
            LOG.debug(" -6 followersCountDvdFriendsCount>2 && biFollowersCount>minRelatedCount/2");
            // 粉丝/关注比例 和 互粉数 很大
            map.insert("basic.followersCountDvdFriendsCount>2 && biFollowersCount>minRelatedCount/2");
            rate -= 6;
        }
        if (biFollowersCount > (minRelatedCount * 3 / 4)) {
            LOG.debug(" -2 biFollowersCount > (minRelatedCount*3/4)");
            map.insert("basic.biFollowersCount > (minRelatedCount*3/4)");
            rate -= 2;
        }
        if (followersCountDvdFriendsCount > 3) {
            LOG.debug(" -2 followersCountDvdFriendsCount>3");
            map.insert("basic.followersCountDvdFriendsCount>3");
            rate -= 2;
        }

        // 基本数值太小
        if ((biFollowersCount < 3 && tweetsCount > 100)
                || (tweetsCount < 3 && (friendsCount + followersCount) > 100)
                || (biFollowersCount < 3 && tweetsCount < 3)) {
            LOG.debug(" +3 basic value too low");
            map.insert("basic.basic.count.low");
            rate += 4;
        }

        if (biFollowersCount < 3 && tweetsCount > 2000) {
            LOG.debug(" +3 tweet number too low");
            map.insert("basic.tweet.count.low");
            rate += 6;
        }

        // 僵尸基本条件：互粉数与最小相关数的比例
        if (biFollowersCount <= minRelatedCount / 10) {
            LOG.debug(" +1 biFollowersCo unt<=minRelatedCount/10");
            map.insert("basic.biFollowersCo unt<=minRelatedCount/10");
            rate += 1;
        }
        if (biFollowersCount <= minRelatedCount / 100) {
            LOG.debug(" +2 biFollowersCount<=minRelatedCount/100");
            map.insert("basic.biFollowersCount<=minRelatedCount/100");
            rate += 2;
        }
        if (followersCountDvdFriendsCount < 1) {
            LOG.debug(" +2 followersCountDvdFriendsCount<1");
            map.insert("basic.followersCountDvdFriendsCount<1");
            rate += 2;
        }
        if (followersCountDvdFriendsCount < 0.5) {
            LOG.debug(" +4 followersCountDvdFriendsCount<0.5");
            map.insert("basic.followersCountDvdFriendsCount<0.5");
            rate += 4;
        }
        if (tweetsCount > 1000 && favouritesCount <= tweetsCount / 10000) {
            LOG.debug(" +2 tweetsCount>1000 && favouritesCount <= tweetsCount/10000");
            map.insert("basic.tweetsCount>1000 && favouritesCount <= tweetsCount/10000");
            rate += 2;
        }
        if (head != null && !AnalyzUtil.hasHead(head)) {
            LOG.debug(" +4 head!=null && !hasHead(head)");
            map.insert("basic.head!=null && !hasHead(head)");
            rate += 4;
        }

        SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMddHHmmss");
        Date a;
        try {
            a = format1.parse(lastTweetTime);
            long tmpTime = a.getTime();
            if (lastTweetTime != null && (tmpTime < System.currentTimeMillis() - SIX_MONTH_TIME)) {
                LOG.debug(" +4 6 month no weibo");
                // 六个月没发微博
                map.insert("basic.six.month.no.weibo");
                rate += 2;
            }
        } catch (ParseException e) {
            LOG.error("last tweet time format error", e);
        } catch (NullPointerException np) {

        }

        if (!StringUtil.isNullOrEmpty(nickname)) {
            if (nickname.startsWith("手机用户") || nickname.startsWith("炫铃用户")) {
                try {
                    Long.parseLong(nickname.substring(4));
                    LOG.debug(" +4 手机用户***");
                    map.insert("basic.nickname.format");
                    rate += 4;
                } catch (Exception ignored) {

                }
            } else if (nickname.startsWith("用户")) {
                try {
                    Long.parseLong(nickname.substring(2));
                    LOG.debug(" +4 用户***");
                    map.insert("basic.nickname.format");
                    rate += 4;
                } catch (Exception ignored) {

                }
            } else if (FeatureUtil.isBadNamePattern(nickname)) {
                LOG.debug(" +4 nickname pattern [英文数字]+中文+数字");
                map.insert("basic.nickname.format");
                rate += 7;
            }
        }


        LOG.debug("basic rate: " + rate);
        map.put("base.rate", rate);
        return new Pair<>(rate, map);
    }


    /**
     * 根据用户所发微博判断僵尸系数
     * 以0为界限：正值越大，表示僵尸系数越高；负值越大，表示真实系数越大
     *
     * @param tweets
     * @return
     */
    private static Pair<Integer, AccumulatorMap> advancedAnalyz(Iterable<UserAnalyzTweetInfo> tweets) {
        AccumulatorMap map = new AccumulatorMap();

        float wbNum = 0;

        Map<String, Float> repostedUserNameCount = new HashMap<String, Float>();
        float badSourceCount = 0;
        float seriousBadSourceCount = 0;
        float absoluteBadSourceCount = 0;
        float badWordsCount = 0;
        float adWordsCount = 0;
        float mktWordsCount = 0;
        float deepRepostCount = 0;
        float hasAttitudeWbCount = 0;
        float hasSignificantRpOrCm = 0;
        float hasRepostOnComment = 0;
        List<Pair<Double, Double>> lngLats = new LinkedList<Pair<Double, Double>>();
        Set<String> srcs = new HashSet<String>();
        Set<String> retweetZmbUids = new HashSet<String>();
        float goodSourceCount = 0;

        float pureRetweetCount = 0;
        float midNightCount = 0;
        float weatherConstellationPatternCount = 0;
        Map<String, Integer> mentionTopicMap = new HashMap<>();

        for (UserAnalyzTweetInfo tweet : tweets) {
            wbNum++;

            // 纯转发
            if (FeatureUtil.isPureRetweet(tweet.getText())) {
                pureRetweetCount++;
            }

            // 午夜微博
            if (FeatureUtil.isMidnightTweet(tweet.getCreateAt())) {
                midNightCount++;
            }

            // 星座和天气
            if (FeatureUtil.isMentionConstellation(tweet.getText()) ||
                    FeatureUtil.isMentionedWeather(tweet.getText())) {
                weatherConstellationPatternCount++;
            }

            // 提及和tipic
            List<String> mention = FeatureUtil.getMentionText(tweet.getText() + tweet.getRetweetText());
            List<String> topic = FeatureUtil.getTopic(tweet.getText() + tweet.getRetweetText());

            for (String s : mention) {
                if (!mentionTopicMap.containsKey(s)) {
                    mentionTopicMap.put(s, 0);
                }
                mentionTopicMap.put(s, mentionTopicMap.get(s) + 1);
            }
            for (String s : topic) {
                if (!mentionTopicMap.containsKey(s)) {
                    mentionTopicMap.put(s, 0);
                }
                mentionTopicMap.put(s, mentionTopicMap.get(s) + 1);
            }


            // 有表态数
            if (tweet.getAttitudeCount() != null && tweet.getAttitudeCount() > 0) {
                hasAttitudeWbCount++;
            }
            // 有不相等的评论或转发
            if (tweet.getCommentCount() / 2 > tweet.getRepostCount() || tweet.getRepostCount() / 2 > tweet.getCommentCount()) {
                hasSignificantRpOrCm++;
            }

            // 有地理位置信息
            // 未过滤 -1 的情况
            if (tweet.getLongtitude() != null && tweet.getLatitude() != null && tweet.getLongtitude() != -1 && tweet.getLatitude() != -1) {
                lngLats.add(new Pair<Double, Double>(tweet.getLongtitude(), tweet.getLatitude()));
            }

            if (tweet.getRetweetText() != null) {
                if (tweet.getRetweetUid() != null) {
                    String rtuid = tweet.getRetweetUid();
                    // repostedUserCount
                    if (!repostedUserNameCount.containsKey(rtuid)) {
                        repostedUserNameCount.put(rtuid, 0F);
                    }
                    repostedUserNameCount.put(rtuid, repostedUserNameCount.get(rtuid) + 1);

                    // deepRepost 3次转发
                    if (StringUtils.countMatches(tweet.getText(), "//@") >= 2) {
                        deepRepostCount++;
                    }

                    // 转发并评论别人的评论
                    if (tweet.getText().startsWith("回复@")) {
                        hasRepostOnComment++;
                    }

                    // 转发了僵尸的微博
                    // 过一遍简单版本的水军
                    if (tweet.getRetweetUser() != null && classifySimple(tweet.getRetweetUser()) == -1) {
                        retweetZmbUids.add(tweet.getRetweetUid());
                    }
                }
            }

            // bad source and bad text
            String source = tweet.getSource();
            if (!StringUtils.isEmpty(source)) {
                if (AnalyzUtil.isBadSource(source)) badSourceCount++;
                if (AnalyzUtil.isSeriousBadSource(source)) seriousBadSourceCount++;
                if (AnalyzUtil.isGoodSource(source)) goodSourceCount++;
                if (AnalyzUtil.isAbsoluteBadSource(source)) absoluteBadSourceCount++;
                if (AnalyzUtil.isBadText(tweet.getText() + " " + tweet.getRetweetText()))
                    badWordsCount++;
                if (AnalyzUtil.isAdText(tweet.getText() + " " + tweet.getRetweetText()))
                    adWordsCount++;
                if (AnalyzUtil.isMarketingWords(tweet.getText()))
                    mktWordsCount++;
                srcs.add(source);
            }
        }
        float rate = 0;
        float tmp;


        if (wbNum <= 5) {
            return new Pair<>(0, map);
        }

        if (goodSourceCount > 0) {
            LOG.debug(" -2 goodSourceCount>0, num: " + goodSourceCount);
            map.insert("advance.good.source");
            rate -= 2;
        }

        if (hasSignificantRpOrCm > 1) {
            LOG.debug(" -2 hasSignificantRpOrCm > 1, num:" + hasSignificantRpOrCm);
            map.insert("advance.unbalanced.repost&comment.ratio");
            rate -= 2;
        }
        if (deepRepostCount > 4) {
            LOG.debug(" -2 deepRepostCount>4, num: " + deepRepostCount);
            map.insert("advance.deep.repost");
            rate -= 2;
        }
        if (hasAttitudeWbCount >= 2) {
            LOG.debug(" -2 hasAttitudeWbCount >= 2, num:" + hasAttitudeWbCount);
            map.insert("advance.biaotai.num");
            rate -= 2;
        }

        if (hasRepostOnComment > 0) {
            LOG.debug(" -2 hasRepostOnComment>0, num: " + hasRepostOnComment);
            map.insert("advance.has.repost.comment");
            rate -= 2;
        }

        int nearLocationsCount = AnalyzUtil.countNearLocations(lngLats);
        if (nearLocationsCount > 0) {
            tmp = nearLocationsCount * 2;
            LOG.debug(" -" + tmp + " nearLocationsCount>0, num: " + nearLocationsCount);
            map.insert("advance.near.location.count");
            rate -= tmp;
        }

        if (pureRetweetCount / wbNum >= 0.4) {
            map.insert("advance.pure.retweet.high");
            rate += 5;
        } else if (pureRetweetCount / wbNum >= 0.6) {
            map.insert("advance.pure.retweet.high");
            rate += 10;
        }

        if (midNightCount > 5) {
            map.insert("advance.midnight.count.5");
            rate += 6;
        } else if (midNightCount > 20) {
            map.insert("advance.midnight.count.20");
            rate += 15;
        }

        if (weatherConstellationPatternCount > 5 || weatherConstellationPatternCount / wbNum > 0.3) {
            map.insert("advance.weather/constelation.pattern");
            rate += 15;
        }

        for (String s : mentionTopicMap.keySet()) {
            if (mentionTopicMap.get(s) / wbNum >= 0.5 && wbNum > 50) {
                map.insert("advance.mention/@.high");
                rate += 15;
            }
        }

        if ((mentionTopicMap.size() / wbNum) > 0.8) {
            map.insert("advance.too.much.mention&topic.0.8");
            rate += 5;
        } else if (mentionTopicMap.size() / wbNum > 1.4) {
            map.insert("advance.too.much.mention&topic.1.4");
            rate += 13;
        }


        if (badSourceCount / wbNum >= 0.7) {
            LOG.debug(" +2 bad source rate > 0.7");
            map.insert("advance.bad.source.rate");
            rate += 2;
        }
        if (seriousBadSourceCount / wbNum >= 0.5) {
            tmp = (seriousBadSourceCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " seriousBadSourceCount rate > 0.5");
            map.insert("advance.serious.bad.source.rate");
            rate += tmp;
        }
        if (absoluteBadSourceCount > 2) {
            LOG.debug(" +=20 absoluteBadSourceCount>2, num: " + absoluteBadSourceCount);
            map.insert("advance.good.source");
            rate += 20;
        }
        if (badWordsCount / wbNum >= 0.5) {
            tmp = (badWordsCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " bad word rate > 0.5, num: " + badWordsCount);
            map.insert("advance.badword&weiboNo.ratio");
            rate += tmp;
        }
        if (adWordsCount / wbNum >= 0.3) {
            tmp = (adWordsCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " adWord rate > 0.3, num: " + adWordsCount);
            map.insert("advance.adWord&weiboNo.ratio");
            rate += tmp;
        }
        if (adWordsCount / wbNum > 0.3 && badWordsCount / wbNum > 0.3) {
            LOG.debug(" +4 seriBadWord rate > 0.3");
            map.insert("advance.seriousBadWord&weiboNo.ratio");
            rate += 4;
        }
        if (mktWordsCount / wbNum > 0.3) {
            tmp = (mktWordsCount / wbNum) * 4;
            LOG.debug(" +" + tmp + " mktWordsCount: " + mktWordsCount);
            map.insert("advance.marketWord&weiboNo.ratio");
            rate += tmp;
        }

        if (retweetZmbUids.size() > 1) {
            tmp = retweetZmbUids.size();
            LOG.debug(" +" + tmp + " retweetZmbUids>1, num: " + retweetZmbUids.size());
            map.insert("advance.retweet.zombie.count");
            rate += tmp;
        }
        LOG.debug(" advanced rate: " + rate);
        return new Pair<>((int) rate, map);
    }

    /**
     * 根据用户所发微博判断僵尸系数
     * 以0为界限：正值越大，表示僵尸系数越高；负值越大，表示真实系数越大
     *
     * @param tweets
     * @return
     */
    private static Pair<Integer, AccumulatorMap> advancedAnalyzBanyan(Iterable<? extends Object> tweets) {
        AccumulatorMap map = new AccumulatorMap();

        float wbNum = 0;

        Map<String, Float> repostedUserNameCount = new HashMap<String, Float>();
        float badSourceCount = 0;
        float seriousBadSourceCount = 0;
        float absoluteBadSourceCount = 0;
        float badWordsCount = 0;
        float adWordsCount = 0;
        float mktWordsCount = 0;
        float deepRepostCount = 0;
        float hasAttitudeWbCount = 0;
        float hasSignificantRpOrCm = 0;
        float hasRepostOnComment = 0;
        List<Pair<Double, Double>> lngLats = new LinkedList<Pair<Double, Double>>();
        Set<String> srcs = new HashSet<String>();
        Set<String> retweetZmbUids = new HashSet<String>();
        float goodSourceCount = 0;

        float pureRetweetCount = 0;
        float midNightCount = 0;
        float weatherConstellationPatternCount = 0;
        Map<String, Integer> mentionTopicMap = new HashMap<>();

        for (Object o : tweets) {
            Map<String, String> p = Util.transform(o);
            String content = p.get("content");
            if (StringUtil.isNullOrEmpty(content))
                continue;

            UserAnalyzTweetInfo tweet = new HBaseWeibo2TweetInfoDocMapper(p).map();
            String srcText = tweet.getRetweetText() != null ? tweet.getRetweetText() : SelfContentExtractor.extractSrcContent(content);
            String text = tweet.getText();
            
            wbNum++;

            // 纯转发
            if (FeatureUtil.isPureRetweet(text)) {
                pureRetweetCount++;
            }

            // 午夜微博
            if (FeatureUtil.isMidnightTweet(tweet.getCreateAt())) {
                midNightCount++;
            }

            // 星座和天气
            if (FeatureUtil.isMentionConstellation(text) ||
                    FeatureUtil.isMentionedWeather(text)) {
                weatherConstellationPatternCount++;
            }

            // 提及和tipic
            List<String> mention = FeatureUtil.getMentionText(content);
            List<String> topic = FeatureUtil.getTopic(content);

            for (String s : mention) {
                if (!mentionTopicMap.containsKey(s)) {
                    mentionTopicMap.put(s, 0);
                }
                mentionTopicMap.put(s, mentionTopicMap.get(s) + 1);
            }
            for (String s : topic) {
                if (!mentionTopicMap.containsKey(s)) {
                    mentionTopicMap.put(s, 0);
                }
                mentionTopicMap.put(s, mentionTopicMap.get(s) + 1);
            }


            // 有表态数
            if (tweet.getAttitudeCount() != null && tweet.getAttitudeCount() > 0) {
                hasAttitudeWbCount++;
            }
            // 有不相等的评论或转发
            if (tweet.getCommentCount() / 2 > tweet.getRepostCount() || tweet.getRepostCount() / 2 > tweet.getCommentCount()) {
                hasSignificantRpOrCm++;
            }

            // 有地理位置信息
            // 未过滤 -1 的情况
            if (tweet.getLongtitude() != null && tweet.getLatitude() != null && tweet.getLongtitude() != -1 && tweet.getLatitude() != -1) {
                lngLats.add(new Pair<Double, Double>(tweet.getLongtitude(), tweet.getLatitude()));
            }

            if (tweet.getRetweetText() != null) {
                if (tweet.getRetweetUid() != null) {
                    String rtuid = tweet.getRetweetUid();
                    // repostedUserCount
                    if (!repostedUserNameCount.containsKey(rtuid)) {
                        repostedUserNameCount.put(rtuid, 0F);
                    }
                    repostedUserNameCount.put(rtuid, repostedUserNameCount.get(rtuid) + 1);

                    // deepRepost 3次转发
                    if (StringUtils.countMatches(text, "//@") >= 2) {
                        deepRepostCount++;
                    }

                    // 转发并评论别人的评论
                    if (text.startsWith("回复@")) {
                        hasRepostOnComment++;
                    }

                    // 转发了僵尸的微博
                    // 过一遍简单版本的水军
                    if (tweet.getRetweetUser() != null && classifySimple(tweet.getRetweetUser()) == -1) {
                        retweetZmbUids.add(tweet.getRetweetUid());
                    }
                }
            }

            // bad source and bad text
            String source = tweet.getSource();
            if (!StringUtils.isEmpty(source)) {
                if (AnalyzUtil.isBadSource(source)) badSourceCount++;
                if (AnalyzUtil.isSeriousBadSource(source)) seriousBadSourceCount++;
                if (AnalyzUtil.isGoodSource(source)) goodSourceCount++;
                if (AnalyzUtil.isAbsoluteBadSource(source)) absoluteBadSourceCount++;
                if (AnalyzUtil.isBadText(content))
                    badWordsCount++;
                if (AnalyzUtil.isAdText(content))
                    adWordsCount++;
                if (AnalyzUtil.isMarketingWords(text))
                    mktWordsCount++;
                srcs.add(source);
            }
        }
        float rate = 0;
        float tmp;


        if (wbNum <= 5) {
            return new Pair<>(0, map);
        }

        if (goodSourceCount > 0) {
            LOG.debug(" -2 goodSourceCount>0, num: " + goodSourceCount);
            map.insert("advance.good.source");
            rate -= 2;
        }

        if (hasSignificantRpOrCm > 1) {
            LOG.debug(" -2 hasSignificantRpOrCm > 1, num:" + hasSignificantRpOrCm);
            map.insert("advance.unbalanced.repost&comment.ratio");
            rate -= 2;
        }
        if (deepRepostCount > 4) {
            LOG.debug(" -2 deepRepostCount>4, num: " + deepRepostCount);
            map.insert("advance.deep.repost");
            rate -= 2;
        }
        if (hasAttitudeWbCount >= 2) {
            LOG.debug(" -2 hasAttitudeWbCount >= 2, num:" + hasAttitudeWbCount);
            map.insert("advance.biaotai.num");
            rate -= 2;
        }

        if (hasRepostOnComment > 0) {
            LOG.debug(" -2 hasRepostOnComment>0, num: " + hasRepostOnComment);
            map.insert("advance.has.repost.comment");
            rate -= 2;
        }

        int nearLocationsCount = AnalyzUtil.countNearLocations(lngLats);
        if (nearLocationsCount > 0) {
            tmp = nearLocationsCount * 2;
            LOG.debug(" -" + tmp + " nearLocationsCount>0, num: " + nearLocationsCount);
            map.insert("advance.near.location.count");
            rate -= tmp;
        }

        if (pureRetweetCount / wbNum >= 0.4) {
            map.insert("advance.pure.retweet.high");
            rate += 5;
        } else if (pureRetweetCount / wbNum >= 0.6) {
            map.insert("advance.pure.retweet.high");
            rate += 10;
        }

        if (midNightCount > 5) {
            map.insert("advance.midnight.count.5");
            rate += 6;
        } else if (midNightCount > 20) {
            map.insert("advance.midnight.count.20");
            rate += 15;
        }

        if (weatherConstellationPatternCount > 5 || weatherConstellationPatternCount / wbNum > 0.3) {
            map.insert("advance.weather/constelation.pattern");
            rate += 15;
        }

        for (String s : mentionTopicMap.keySet()) {
            if (mentionTopicMap.get(s) / wbNum >= 0.5 && wbNum > 50) {
                map.insert("advance.mention/@.high");
                rate += 15;
            }
        }

        if ((mentionTopicMap.size() / wbNum) > 0.8) {
            map.insert("advance.too.much.mention&topic.0.8");
            rate += 5;
        } else if (mentionTopicMap.size() / wbNum > 1.4) {
            map.insert("advance.too.much.mention&topic.1.4");
            rate += 13;
        }


        if (badSourceCount / wbNum >= 0.7) {
            LOG.debug(" +2 bad source rate > 0.7");
            map.insert("advance.bad.source.rate");
            rate += 2;
        }
        if (seriousBadSourceCount / wbNum >= 0.5) {
            tmp = (seriousBadSourceCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " seriousBadSourceCount rate > 0.5");
            map.insert("advance.serious.bad.source.rate");
            rate += tmp;
        }
        if (absoluteBadSourceCount > 2) {
            LOG.debug(" +=20 absoluteBadSourceCount>2, num: " + absoluteBadSourceCount);
            map.insert("advance.good.source");
            rate += 20;
        }
        if (badWordsCount / wbNum >= 0.5) {
            tmp = (badWordsCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " bad word rate > 0.5, num: " + badWordsCount);
            map.insert("advance.badword&weiboNo.ratio");
            rate += tmp;
        }
        if (adWordsCount / wbNum >= 0.3) {
            tmp = (adWordsCount / wbNum) * 8;
            LOG.debug(" +" + tmp + " adWord rate > 0.3, num: " + adWordsCount);
            map.insert("advance.adWord&weiboNo.ratio");
            rate += tmp;
        }
        if (adWordsCount / wbNum > 0.3 && badWordsCount / wbNum > 0.3) {
            LOG.debug(" +4 seriBadWord rate > 0.3");
            map.insert("advance.seriousBadWord&weiboNo.ratio");
            rate += 4;
        }
        if (mktWordsCount / wbNum > 0.3) {
            tmp = (mktWordsCount / wbNum) * 4;
            LOG.debug(" +" + tmp + " mktWordsCount: " + mktWordsCount);
            map.insert("advance.marketWord&weiboNo.ratio");
            rate += tmp;
        }

        if (retweetZmbUids.size() > 1) {
            tmp = retweetZmbUids.size();
            LOG.debug(" +" + tmp + " retweetZmbUids>1, num: " + retweetZmbUids.size());
            map.insert("advance.retweet.zombie.count");
            rate += tmp;
        }
        LOG.debug(" advanced rate: " + rate);
        return new Pair<>((int) rate, map);
    }

}

