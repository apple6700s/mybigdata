package com.datastory.banyan.migrate1;

import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.MsgTypeAnalyzer;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.datastory.banyan.weibo.util.WeiboUtils;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.*;

/**
 * com.datastory.banyan.migrate1.WbContentMigrateSpark
 *
 * @author lhfcws
 * @since 16/11/21
 */

public class WbContentMigrateSpark extends Hb2PhMigrateSparkTemplate {
    public WbContentMigrateSpark() {
        super(
                "dt.rhino.weibo.content.v2",
                "DS_BANYAN_WEIBO_CONTENT_V1",
                "mid",
                new String[]{
                        "pk", "update_date", "uid"
                },
                new String[]{
                        "mid", "create_date", "text", "src_mid", "repost_mid", "source",
                        "reposts_count", "comments_count", "attitudes_count", "geo", "feature",
                        "pic_urls", "original_pic", "annotations", "other_data", "sentiment",
                        "keywords", "fingerprint", "topics", "emoji", "short_link", "original_link",
                        "checkin", "mention", "forward", "is_ad",
                },
                new String[]{
                        "mid", "publish_date", "content", "src_mid", "rt_mid", "source",
                        "reposts_cnt", "comments_cnt", "attitudes_cnt", "geo", "feature",
                        "pic_urls", "original_pic", "annotations", "other_data", "sentiment",
                        "keywords", "fingerprint", "topics", "emoji", "short_link", "original_link",
                        "footprint", "mention", "forward", "is_ad",
                }
        );
    }

    @Override
    public Params customizedValue(Params p) {
        // self_content
        String selfContent = SelfContentExtractor.extract(p.getString("content"));
        if (selfContent != null)
            p.put("self_content", selfContent);

        BanyanTypeUtil.safePut(p, "url", WeiboUtils.getWeiboUrl(p.getString("uid"), p.getString("mid")));

//        String source = p.getString("source");
//        List<String> sourceList = SourceExtractor.extractSources(source);
//        if (!CollectionUtil.isEmpty(sourceList))
//            BanyanTypeUtil.safePut(p, "source", StringUtils.join(sourceList, "|"));

        // msg_type
        short msgType = MsgTypeAnalyzer.analyz(
                p.getString("src_mid"),
                p.getString("rt_mid"),
                selfContent,
                p.getString("content")
        );
        p.put("msg_type", "" + msgType);

        return p;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        WbContentMigrateSpark spark = new WbContentMigrateSpark();

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
