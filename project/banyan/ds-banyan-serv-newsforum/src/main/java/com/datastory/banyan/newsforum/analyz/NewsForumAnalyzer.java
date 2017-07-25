package com.datastory.banyan.newsforum.analyz;

import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.yeezhao.commons.util.Entity.Params;


/**
 * com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumAnalyzer extends CommonLongTextAnalyzer {
    protected static final String R_FILTER_IMG_PRINT = "QQ浏览器截图_\\d+_[a-zA-Z\\d]+\\.[jJ][pP][gG] \\([\\d. ]+KB, 下载次数: \\d+\\) 下载附件 [\\S ]+ 上传";

    private static volatile NewsForumAnalyzer _singleton = null;

    public static NewsForumAnalyzer getInstance() {
        if (_singleton == null)
            synchronized (NewsForumAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumAnalyzer();
                }
            }
        return _singleton;
    }

    public Params _beforeAnalyz(Params p) {
        String content = p.getString("content");
        if (content != null) {
            content = content.replaceAll(R_FILTER_IMG_PRINT, "");
            p.put("content", content);
        }
        return p;
    }
}
