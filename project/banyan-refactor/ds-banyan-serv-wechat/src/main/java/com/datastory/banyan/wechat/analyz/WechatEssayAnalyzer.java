package com.datastory.banyan.wechat.analyz;

import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.wechat.analyz.WechatEssayAnalyzer
 *
 * @author lhfcws
 * @since 16/12/13
 */

public class WechatEssayAnalyzer extends CommonLongTextAnalyzer {
    private static volatile WechatEssayAnalyzer _singleton = null;

    public static WechatEssayAnalyzer getInstance(boolean enableLog) {
        if (_singleton == null)
            synchronized (WechatEssayAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new WechatEssayAnalyzer(enableLog);
                }
            }
        return _singleton;
    }

    public static WechatEssayAnalyzer getInstance() {
        return getInstance(false);
    }

    public WechatEssayAnalyzer() {
    }

    public WechatEssayAnalyzer(boolean enableLog) {
        super(enableLog);
    }

    @Override
    public Params _beforeAnalyz(Params p) {
        String url = p.getString("url");
        String articleId = WxArticleIdGen.genId(url);
        if (articleId != null) {
            p.put("article_id", articleId);
        }
        return p;
    }
}
