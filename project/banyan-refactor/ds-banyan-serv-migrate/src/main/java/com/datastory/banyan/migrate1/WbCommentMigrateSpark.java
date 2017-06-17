package com.datastory.banyan.migrate1;

import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Date;

/**
 * com.datastory.banyan.migrate1.WbCommentMigrateSpark
 *
 * @author lhfcws
 * @since 16/12/15
 */

public class WbCommentMigrateSpark extends Hb2PhMigrateSparkTemplate {
    public WbCommentMigrateSpark() {
        super(
                "dt.rhino.weibo.comment",
                "DS_BANYAN_WEIBO_COMMENT",
                "comment_id",
                new String[] {},
                new String[] {
                        "comment_id", "mid", "content", "uid", "update_date", "publish_date", "sentiment", "nickname", "keywords", "fingerprint"
                },
                new String[] {
                        "cmt_mid", "mid", "content", "uid", "update_date", "publish_date", "sentiment", "name", "keywords", "fingerprint"
                }
        );
        families = new byte[][]{"raw".getBytes()};
    }

    public Params customizedValue(Params p) {
        if (!p.containsKey("keywords"))
            p = WbUserAnalyzer.getInstance().migrateAnalyz(p);
        return p;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new WbCommentMigrateSpark().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
