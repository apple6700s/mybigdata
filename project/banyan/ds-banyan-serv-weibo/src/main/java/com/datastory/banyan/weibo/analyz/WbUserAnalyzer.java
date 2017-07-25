package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.analyz.*;
import com.datastory.banyan.weibo.doc.scavenger.ExtEntityDocMapper;
import com.datastory.banyan.weibo.doc.scavenger.HBaseParams2UserInfoDocMapper;
import com.datastory.banyan.utils.DateUtils;
import com.datatub.scavenger.extractor.EntityExtractor;
import com.datatub.scavenger.model.ExtEntity;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.hornbill.algo.astrotufing.classifier.AstrotufingClassfier;
import com.yeezhao.hornbill.algo.astrotufing.util.CoreConsts;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzBasicInfo;

import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.WbUserAnalyzer
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class WbUserAnalyzer {
    private static volatile WbUserAnalyzer _singleton = null;

    public static WbUserAnalyzer getInstance() {
        if (_singleton == null)
            synchronized (WbUserAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new WbUserAnalyzer();
                }
            }
        return _singleton;
    }

    public static int scavRes2userType(int scavRes) {
        if (scavRes == -1)
            return CoreConsts.USER_TYPE.ROBOT.getCode();
        else if (scavRes == 0)
            return CoreConsts.USER_TYPE.UNK.getCode();
        else
            return CoreConsts.USER_TYPE.NORMAL.getCode();
    }

    public static int userType2isReal(int type) {
        if (type != CoreConsts.USER_TYPE.ROBOT.getCode())
            return 1;
        else
            return 0;
    }

    public static int userType2isRobot(int type) {
        if (type != CoreConsts.USER_TYPE.ROBOT.getCode())
            return 0;
        else
            return 1;
    }


    public int analyzUserType(Params user) {
        if (user == null)
            return CoreConsts.USER_TYPE.UNK.getCode();
        UserAnalyzBasicInfo userAnalyzBasicInfo = new HBaseParams2UserInfoDocMapper(user).map();
        return analyzUserType(userAnalyzBasicInfo);
    }

    public int analyzUserType(UserAnalyzBasicInfo user) {
        if (user == null)
            return CoreConsts.USER_TYPE.UNK.getCode();
        AstrotufingClassfier classfier = new AstrotufingClassfier();
        int scavRes;
        if (CollectionUtil.isEmpty(user.getTweets()))
            scavRes = classfier.classifySimple(user);
        else
            scavRes = classfier.classify(user);
        return scavRes2userType(scavRes);
    }

    protected String getAllContent(Params user) {
        List<Params> tweets = user.getList("_tweets", Params.class);
        if (CollectionUtil.isEmpty(tweets))
            return null;

        StringBuilder allContentSB = new StringBuilder();
        for (Params tweet : tweets) {
            if (tweet.get("content") != null) {
                allContentSB.append(tweet.getString("content")).append(" ");
            }
        }
        if (allContentSB.length() == 0)
            return null;

        String allContent = allContentSB.toString();
        return allContent;
    }

    public Params allAnalyz(Params user) {
        user = analyz(user);
        user = advAnalyz(user);
        return user;
    }

    @Deprecated
    public Params advAnalyzNStrip(Params user) {
        if (user == null || user.isEmpty())
            return null;
        Params extUser = new Params();
        extUser.put("pk", user.getString("pk"));

        UserAnalyzBasicInfo userAnalyzBasicInfo = new HBaseParams2UserInfoDocMapper(user).map();

        // user_type
        int userType = analyzUserType(userAnalyzBasicInfo);

        // activeness
//        ScavengerConsts.USER_ACTIVENESS activeness = new ActivenessExtractor().extract(userAnalyzBasicInfo);

        extUser.put("user_type", userType);
//        extUser.put("activeness", activeness.getValue());

        // analyz from content
        String allContent = getAllContent(user);
        if (!StringUtil.isNullOrEmpty(allContent)) {
            // take result from ExtEntity
            ExtEntity ext = EntityExtractor.extract(allContent);
            Params p = new ExtEntityDocMapper(ext).map();
            extUser.putAll(p);
        }

        return extUser;
    }

    @Deprecated
    public Params advAnalyz(Params user) {
        if (user == null || user.isEmpty())
            return user;

        Params advUser = advAnalyzNStrip(user);
        if (advUser != null)
            user.putAll(advUser);

        return user;
    }

    public Params migrateAnalyz(Params user) {
        if (user == null)
            return null;

        int g = GenderTranslator.translate(user.getString("gender"));
        user.put("gender", g + "");

        // last_tweet_date
        if (user.get("last_tweet_date") == null)
            user.put("last_tweet_date", DateUtils.getCurrentTimeStr());

        // user_type
        if (!user.containsKey("user_type")) {
            int userType = analyzUserType(user);
            user.put("user_type", userType);
        }

        // birth
        if (user.get("birthdate") != null) {
            String birthDate = user.getString("birthdate");
            String birthYear = BirthYearExtractor.extract(birthDate);
            Constellation constellation = Constellation.identifyConstellation(birthDate);
            if (birthYear != null)
                user.put("birthyear", birthYear);
            if (constellation != null)
                user.put("constellation", constellation.getName());
        }

        // vtype
        if (user.get("verified_type") != null) {
            VType vType = VType.fromOriginTypeStr(user.getString("verified_type"));
            if (vType != null)
                user.put("vtype", vType.getType());
        }

        // city & location
        if (user.get("province") != null && user.get("city") != null) {
            String city = AreaTranslator.getCity(user.getString("province"), user.getString("city"));
            if (city != null) {
                String cityLevel = AreaTranslator.getCityLevel(city);
                user.put("city_level", cityLevel);
            }
        }

        return user;
    }

    public Params analyz(Params user) {
        if (user == null)
            return null;

        int g = GenderTranslator.translate(user.getString("gender"));
        user.put("gender", g + "");

        // last_tweet_date
        if (user.get("last_tweet_date") == null)
            user.put("last_tweet_date", DateUtils.getCurrentTimeStr());

        // user_type
//        int userType = analyzUserType(user);
//        user.put("user_type", userType);

        // birth
        if (user.get("birthdate") != null) {
            String birthDate = user.getString("birthdate");
            String birthYear = BirthYearExtractor.extract(birthDate);
            Constellation constellation = Constellation.identifyConstellation(birthDate);
            if (birthYear != null)
                user.put("birthyear", birthYear);
            if (constellation != null)
                user.put("constellation", constellation.getName());
        }

        // vtype
        if (user.get("verified_type") != null) {
            VType vType = VType.fromOriginTypeStr(user.getString("verified_type"));
            if (vType != null)
                user.put("vtype", vType.getType());
        }

        // city & location
        if (user.get("province") != null && user.get("city") != null) {
            String city = AreaTranslator.getCity(user.getString("province"), user.getString("city"));
            if (city != null) {
                String cityLevel = AreaTranslator.getCityLevel(city);
                user.put("city_level", cityLevel);
            }
        }

        if (user.getString("fans_cnt") != null) {
            user.put("fans_level", FansLevelTranslator.getFollowersCountRank(user.getString("fans_cnt")));
        }

        return user;
    }

}
