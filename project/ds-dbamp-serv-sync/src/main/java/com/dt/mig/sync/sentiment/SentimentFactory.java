package com.dt.mig.sync.sentiment;

/**
 * Created by abel.chan on 16/11/20.
 */
public class SentimentFactory {

    private static ISentiment iLongTextSentiment = null;

    public static ISentiment getLongTextInstance() {
        if (iLongTextSentiment == null) {
            synchronized (SentimentFactory.class) {
                if (iLongTextSentiment == null) {
                    iLongTextSentiment = SentimentFactory.buildSentimentObj(SentimentFactory.SentimentType.LongText);
                }
            }
        }
        return iLongTextSentiment;
    }


    public static ISentiment buildSentimentObj(SentimentType type) {

        if (SentimentType.LongText == type) {

            ISentiment iSentiment = new TextSentiment();
            iSentiment.setUp();//条用setup初始化参数

            return iSentiment;
        }


//        if (SentimentType.LongText == type) {
//            if (iSentiment == null) {
//                synchronized (SentimentFactory.class) {
//                    if (iSentiment == null) {
//                        iSentiment = new TextSentiment();
//                        iSentiment.setUp();//条用setup初始化参数
//                    }
//                }
//            }
//        } else {
//            return null;
//        }

        return null;
    }

    public enum SentimentType {
        LongText
    }

    public static void main(String[] args) {
//        ISentiment sentiment = SentimentFactory.buildSentimentObj(SentimentType.LongText);
////        String value = "#烧脑24小时# 先看视频在做题提交答案B、C、B、B";
//        //String value = "用微博查询用户的内容";
//        String value = "骚扰电话,骚扰电话,骚扰电话,骚扰电话,骚扰电话,骚扰电话,骚扰电话,骚扰电话,骚扰电话,";
//        //System.out.println(sentiment.removeSentenceWithRegex(value));
//        System.out.println(sentiment.senAnaly(value));
//
//        //String value = "用微博查询用户的内容";
//        value ="据腾讯手机管家提供的拦截数据显示，日前“奥运赌博”诈骗短信呈现猖獗势头。 由于赌博本身涉嫌违法，加之钓鱼网址或不安全，腾讯手机管家安全专家提醒用户在收到此类短信时，不可轻信参与投注，否则将有可能遭遇财产损失。 里约奥运会开幕前，腾讯手机管家即查杀了 “奥运会”、“奥运赛程”、“奥运夺冠”和“奥运百科”4款被病毒依附的奥运软件。 其次，想要从根源上“消灭”诈骗短信等安全威胁，用户可以选择更加直截了当的防御方法，安装腾讯手机管家等专业安全软件，自动识别和拦截可疑号码发来的垃圾短信。 譬如，当手机收到“奥运投注”的诈骗短信时，腾讯手机管家可以标记为“涉嫌违法”并自主拦截，直接将安全威胁“拒之门外”。 借助腾讯安全云库的海量大数据优势以及独创的伪基站识别技术，腾讯手机管家的骚扰短信拦截率已高达98%。 (图：腾讯手机管家精准拦截“奥运投注”诈骗短信) 　　为了让用户彻底远离不法分子攻击，腾讯手机管家还设置了“二次保护”。 如若用户不慎点击了“奥运赌博”诈骗短信中的钓鱼网址，网页会自动弹出标有“这可能是非法博彩网站，建议开启手机管家，全面保护手机安全”的小窗口，有效保护手机和个人财产安全。 (图：腾讯手机管家对风险网址进行用户提醒) 　　值得一提的是，腾讯手机管家不仅让用户安全看奥运，还带来了更酷的奥运观看“姿势”，其为里约奥运会定制推出的“智能闹钟”，可以一键掌握热点赛事和金牌动向。 据了解，安卓和苹果用户安装腾讯手机管家6.5(Android)版本或腾讯手机管家6.4(iPhone)版本，分别开启“奥运赛事提醒”和“奥运赛程闹钟”功能，就可以即时获得热门赛事提醒，掌握第一手金牌资讯。";
//        //System.out.println(sentiment.removeSentenceWithRegex(value));
//        System.out.println(sentiment.senAnaly(value));
    }
}
