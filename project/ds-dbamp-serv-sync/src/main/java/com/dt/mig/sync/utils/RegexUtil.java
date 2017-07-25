package com.dt.mig.sync.utils;

import java.util.regex.Pattern;

/**
 * Created by abel.chan on 17/2/16.
 */
public class RegexUtil {


    // 定义HTML标签的正则表达式
    //public static final Pattern PATTERN_REGEX_HTML = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE);


    //提取at符号
    public static final String REGEX_METION = "@([0-9a-zA-Z一-龥_-]+)([^/.0-9a-zA-Z一-龥_-]|$)";

    //微博内容提取器,剔除话题
    public static final String REGEX_TOPIC_TAG = "#[^\\#|.]+#";

    //提取表情的字符串
    public final static String REGEX_EXPRESSION = "\\[[\\u4e00-\\u9fa5a-zA-Z0-9]+\\]";

    //匹配文字中的图片截屏的文字
    public final static String RRGEX_IMG_PATH = "(QQ浏览器截屏未命名\\d+.[jJPp][pPNn][gG]|QQ浏览器截图_\\d+_[a-zA-Z\\d]+\\.[jJ][pP][gG] \\([\\d. ]+KB, 下载次数: \\d+\\) 下载附件 [\\S ]+ 上传)";

    // 定义HTML标签的正则表达式
    public final static String RRGEX_HTML = "<[^>]+>";

    //提取at符号
    public final static Pattern PATTERN_REGEX_METION = Pattern.compile(REGEX_METION);

    // 提取是否QQ浏览器的推送新闻。
    public final static Pattern PATTERN_REGEX_QQBROWSER_NEWS = Pattern.compile("(@QQ浏览器|@手机QQ浏览器)\\s+(https?://(w{3}\\.)?)?\\w+\\.\\w+(\\.[a-zA-Z]+)*(:\\d{1,5})?(/\\w*)*(\\??(.+=.*)?(&.+=.*)?)?");

}
