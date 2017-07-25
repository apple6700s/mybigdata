package com.dt.mig.sync.utils;

import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;

/**
 * Created by abel.chan on 17/2/12.
 */
public class SentenceUtil {

    public static final int MAX_RANDOM_VALUE = 5;

    public static final List<String> SPECIAL_WORDS = Arrays.asList("垃圾短信", "骚扰拦截", "反诈骗", "诈骗电话");

    /**
     * WEIBO
     * 提取@的字段,如, @Meme 今天 => Meme
     *
     * @param str
     * @return
     */
    public static List<String> extractAtSymbol(String str) {
        Matcher matcher = RegexUtil.PATTERN_REGEX_METION.matcher(str);
        return getMacherList(matcher, 1);
    }

    /**
     * WEIBO
     * 提取@的字段,如, @Meme 今天 => Meme
     *
     * @param str
     * @return
     */
    public static String removeAtSymbol(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return str;
        }
        return new String(str).replaceAll(RegexUtil.REGEX_METION, "");
    }

    /**
     * WEIBO
     * 提取@的字段,如, @Meme 今天 => Meme
     *
     * @param str
     * @return
     */
    public static boolean checkIsQQBrowserNews(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return false;
        }
        return RegexUtil.PATTERN_REGEX_QQBROWSER_NEWS.matcher(str.trim()).find();
    }

    /**
     * 过滤掉内容中的表情
     *
     * @param str
     * @return
     */
    public static String removeExpression(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return str;
        }
        return new String(str).replaceAll(RegexUtil.REGEX_EXPRESSION, "");
    }

    /**
     * 过滤掉内容中的html标签
     *
     * @param str
     * @return
     */
    public static String removeHtmlTag(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return str;
        }
        return new String(str).replaceAll(RegexUtil.RRGEX_HTML, "");
    }

    /**
     * 过滤掉内容中的图片截屏文字
     *
     * @param str
     * @return
     */
    public static String removeImgPath(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return str;
        }
        return new String(str).replaceAll(RegexUtil.RRGEX_IMG_PATH, "");
    }


    public static boolean isIncludeSpecialWord(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            return false;
        }
        for (String specialWord : SPECIAL_WORDS) {
            if (str.contains(specialWord)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRandomPostive() {
        Random random = new Random();
        return random.nextInt(MAX_RANDOM_VALUE) != 0;
    }

    public static void main(String[] args) {

        String value = "【轻乳酪蛋糕（低油低糖版）的做法】轻乳酪蛋糕（低油低糖版）怎么做_轻乳酪蛋糕（低油低糖版）的家常做法_下厨房@手机QQ1浏览器1 http://t.cn/RcrVemG";
        System.out.println(checkIsQQBrowserNews(value));

        value = "【轻乳酪蛋糕（低油低糖版）的做法】轻乳酪蛋糕（低油低糖版）怎么做_轻乳酪蛋糕（低油低糖版）的家常做法_下厨房手机QQ浏览器 http://t.cn/RcrVemG";
        System.out.println(checkIsQQBrowserNews(value));

        value = "【轻乳酪蛋糕（低油低糖版）的做法】轻乳酪蛋糕（低油低糖版）怎么做_轻乳酪蛋糕（低油低糖版）的家常做法_下厨房@QQ浏览器 http://t.cn/RcrVemG";
        System.out.println(checkIsQQBrowserNews(value));

        value = "莫名刘诗诗被诅咒，被点蜡，狮子背黑锅，这件事绝对不轻易结束。\\n吴奇隆先生大粉带头泼脏水，刘诗诗被人身数千条，更有恶毒的人直接艾特刘诗诗进行辱骂！语言极其恶毒！\\n贴脸拍刘诗诗的吴奇隆粉丝@秦小吉 →http://weibo.com/u/1737464944\\n吴奇隆粉丝自己拍的视频，离刘诗诗多近！这只是一段[微笑]...全文： http://m.weibo.cn/5890643645/3997837148329752";
        System.out.println(removeExpression(value));
        System.out.println(value);

        List<String[]> keywords = Arrays.asList(new String[]{"腾讯", "手机管家"}, new String[]{"豌豆荚"});
        value = "//@jlijames:黑名单吧 //@吕宏伟:品德老师本职工作 @小沐豌豆荚 //@368天: //@小儿外科裴医生: 这个还真是，这位遂宁的小学品德老师，不但在网上多次喊过要砍死医生，还要让名字和她前男友一样的学生统统不及格。   http://t.cn/RV7rdiZ //@沐沐豌豆荚：当老师的戾气这么重，可怜了她手下那些孩子了……";
        System.out.println(isNeedSentiment(value, keywords));
        System.out.println(value);

        value = "//@jlijames:黑名单吧 //@吕宏伟:品德老师本职工作 @豌豆荚小沐 //@368天: //@小儿外科裴医生: 这个还真是，这位遂宁的小学品德老师，不但在网上多次喊过要砍死医生，还要让名字和她前男友一样的学生统统不及格。   http://t.cn/RV7rdiZ //@沐沐豌豆荚：当老师的戾气这么重，可怜了她手下那些孩子了……";
        System.out.println(isNeedSentiment(value, keywords));
        System.out.println(value);

        value = "//@jlijames:黑名单吧 //@吕宏伟:品德老师本职工作 @豌豆荚 //@368天: //@小儿外科裴医生: 这个还真是，这位遂宁的小学品德老师，不但在网上多次喊过要砍死医生，还要让名字和她前男友一样的学生统统不及格。   http://t.cn/RV7rdiZ //@沐沐豌豆荚：当老师的戾气这么重，可怜了她手下那些孩子了……";
        System.out.println(isNeedSentiment(value, keywords));
        System.out.println(value);

        value = "//@jlijames:黑名单吧 //@吕宏伟:品德老师本职工作 @豌豆荚1 //@368天: //@小儿外科裴医生: 这个还真是，这位遂宁的小学品德老师，不但在网上多次喊过要砍死医生，还要让名字和她前男友一样的学生统统不及格。   http://t.cn/RV7rdiZ //@沐沐豌豆荚：当老师的戾气这么重，可怜了她手下那些孩子了……";
        System.out.println(isNeedSentiment(value, keywords));
        System.out.println(value);

        value = "//@jlijames:黑名单吧 //@吕宏伟:品德老师本职工作 @手机管家腾讯 //@368天: //@小儿外科裴医生: 这个还真是，这位遂宁的小学品德老师，不但在网上多次喊过要砍死医生，还要让名字和她前男友一样的学生统统不及格。   http://t.cn/RV7rdiZ //@沐沐豌豆荚：当老师的戾气这么重，可怜了她手下那些孩子了……";
        System.out.println(isNeedSentiment(value, keywords));
        System.out.println(value);

        value = "2015-2016袭警报告：每18小时发生一起袭警事件 - 手机新蓝网@手机QQ浏览器 http://t.cn/RJHtcaA最近袭警辱警事件越来越多，规范执法迫在眉睫，中国公民只知道维护权益不知道还有义务，执法者要全程录像，先出示证件，对不配合的可采取强制措施，对袭警者可当场击毙，学学国外吧！";
        System.out.println(value);
        System.out.println(checkIsQQBrowserNews(value));

        value = "回复@PC_腾讯QQ浏览器:是开机自动弹出这个窗口，联系方式已经私信给你了。";
        System.out.println(value);
        keywords = Arrays.asList(new String[]{"QQ浏览器"}, new String[]{"器哥"});
        System.out.println(isNeedSentiment(value, keywords));

        value = "回复@腾讯QQ浏览器:是开机自动弹出这个窗口，联系方式已经私信给你了。";
        System.out.println(value);
        keywords = Arrays.asList(new String[]{"QQ浏览器", "腾讯"}, new String[]{"器哥"});
        System.out.println(isNeedSentiment(value, keywords));

        value = "QQ浏览器回复@PC_QQ浏览器:是开机自动弹出这个窗口，联系方式已经私信给你了。";
        System.out.println(value);
        keywords = Arrays.asList(new String[]{"QQ浏览器"}, new String[]{"器哥"});
        System.out.println(isNeedSentiment(value, keywords));

        value = "QQ浏览器回复@PC_ 器:是开机自动弹出这个窗腾讯口，联系方式已经私信给你了。";
        System.out.println(value);
        keywords = Arrays.asList(new String[]{"QQ浏览器", "腾讯"}, new String[]{"器哥"});
        System.out.println(isNeedSentiment(value, keywords));

        value = "QQ浏览器回复@PC_ 器:是开机自动弹出这个窗口，联系方式已经私信给你了。";
        System.out.println(value);
        keywords = Arrays.asList(new String[]{"QQ浏览器", "腾讯"}, new String[]{"器哥"});
        System.out.println(isNeedSentiment(value, keywords));

    }


    /**
     * 将对应group的所有match的对象全部返回
     *
     * @param matcher
     * @param index
     * @return
     */
    private static List<String> getMacherList(Matcher matcher, int index) {
        List<String> rst = new ArrayList<String>();
        while (matcher.find()) {
            rst.add(matcher.group(index));
        }
        return rst;
    }

    public static boolean isIncludeKeyWordExcludeAtSymb(String text, List<String[]> keywords) {
        String val = removeAtSymbol(text);
        if (StringUtils.isEmpty(val)) {
            return false;
        }

        for (String[] orKeywords : keywords) {
            boolean isFind = true;
            for (String andKeyword : orKeywords) {
                if (!val.contains(andKeyword)) {
                    isFind = false;
                    break;
                }
            }
            if (isFind) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断是否需要进行情感判断
     *
     * @param text
     * @return
     */
    public static boolean isNeedSentiment(String text, List<String[]> keywords) {
        if (StringUtil.isNullOrEmpty(text)) {
            return false;
        }

        //检查除了@对象外,是否存在关键词 ,检查@对象,如果对象满足关键词的,则返回true;否则false;
        if (!isIncludeKeyWordExcludeAtSymb(text, keywords) && !checkAtObj(text, keywords)) return false;

        //QQ浏览器+网页链接
        //if (checkIsQQBrowserNews(text)) return false;

        return true;
    }

    /**
     * 检查@对象是否是满足关键词的
     *
     * @param text
     * @param keywords
     * @return
     */
    private static boolean checkAtObj(String text, List<String[]> keywords) {
        List<String> list = extractAtSymbol(text);
        if (list == null || list.size() == 0) {
            return true;//说明没有@符号,这一轮需要被跳过
        }

        if (list != null && list.size() > 0) {
            for (String value : list) {
                for (String[] orKeywords : keywords) {
                    String tmp = new String(value.trim());
                    for (String andKeyword : orKeywords) {
                        tmp = tmp.replace(andKeyword, "");
                    }
                    if (StringUtils.isEmpty(tmp)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}