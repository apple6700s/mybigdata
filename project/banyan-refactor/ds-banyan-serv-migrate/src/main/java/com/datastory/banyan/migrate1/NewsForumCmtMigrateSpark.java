package com.datastory.banyan.migrate1;


import com.datastory.banyan.analyz.HTMLTrimmer;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.Date;

/**
 * com.datastory.banyan.migrate1.NewsForumCmtMigrateSpark
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class NewsForumCmtMigrateSpark extends Hb2PhMigrateSparkTemplate {

    public NewsForumCmtMigrateSpark() {
        super("dt.rhino.sys.common.v7", "DS_BANYAN_NEWSFORUM_COMMENT_V1", "item_id",
                new String[]{
                        "pk", "update_date", "parent_id.parent_post_id"
                },
                new String[]{
                        "title", "content", "author", "keywords", "sentiment", "fingerprint", "publish_date",
                        "sourceCrawlerId", "taskId", "url", "source", "is_ad", "is_robot", "cat_id", "is_main_post",
                },
                new String[]{
                        "title", "content", "author", "keywords", "sentiment", "fingerprint", "publish_date",
                        "sourceCrawlerId", "taskId", "url", "source", "is_ad", "is_robot", "cat_id", "is_main_post",
                }
        );
    }

    @Override
    public String genPK(String s) {
        return s;
    }

    @Override
    public Params filterParams(Params p) {
        if (p.get("is_main_post") != null && p.get("is_main_post").equals("0"))
            return p;
        else {
            if (p.get("parent_id") != null)
                return p;
        }
        return null;
    }

    @Override
    public Params customizedValue(Params p) {
        p = HTMLTrimmer.trim(p, "content");
        p = HTMLTrimmer.trim(p, "title");
        p = HTMLTrimmer.trim(p, "author");
        return p;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        NewsForumCmtMigrateSpark spark = new NewsForumCmtMigrateSpark();

        FilterList filterList = new FilterList();
        if (args.length >= 1) {
            String startUpdateDate = args[0];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }

        if (args.length >= 2) {
            String endUpdateDate = args[1];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, endUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }
        if (filterList.getFilters().isEmpty())
            spark.run();
        else
            spark.run(filterList);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
