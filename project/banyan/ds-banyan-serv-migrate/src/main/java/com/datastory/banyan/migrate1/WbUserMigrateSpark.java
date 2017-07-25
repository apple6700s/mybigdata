package com.datastory.banyan.migrate1;

import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.Date;

/**
 * com.datastory.banyan.migrate1.WbUserMigrateSpark
 *
 * @author lhfcws
 * @since 16/11/21
 */

public class WbUserMigrateSpark extends Hb2PhMigrateSparkTemplate {
    public WbUserMigrateSpark() {
        super(
                "dt.rhino.weibo.user.v2",
                "DS_BANYAN_WEIBO_USER",
                "uid",
                new String[]{
                        "pk", "update_date"
                },
                new String[]{
                        "uid", "name", "province", "city", "desc", "blog_url", "img_url", "url",  "gender",
                        "wb_cnt", "fav_cnt", "follow_cnt", "friend_cnt", "bi_followers_count", "create_date", "birthdate",
                        "other_data", "verifiedType", "meta_group", "company", "school", "tags", "sources", "urank", "topics",
                        "user_type", "location", "location_format", "weihao", "domain", "activeness", "verified_reason",
                        "footprints", "user_status", "group_name", "tag_dist", "vtype",
                },
                new String[]{
                        "uid", "name", "province", "city", "desc", "blog_url", "head_url", "url",  "gender",
                        "wb_cnt", "fav_cnt", "fans_cnt", "follow_cnt", "bi_follow_cnt", "publish_date", "birthdate",
                        "other_data", "verified_type", "meta_group", "company", "school", "tags", "sources", "urank", "topics",
                        "user_type", "location", "location_format", "weihao", "domain", "activeness", "verified_reason",
                        "footprints", "user_status", "group", "tag_dist", "vtype",
                }
        );
    }

    @Override
    public Params customizedValue(Params p) {
        p = WbUserAnalyzer.getInstance().migrateAnalyz(p);

        return p;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        WbUserMigrateSpark spark = new WbUserMigrateSpark();
        WbUserFollowMigrator fmig = new WbUserFollowMigrator();

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
        if (filterList.getFilters().isEmpty()) {
            spark.run();
            System.out.println("Run follow migrate. " + new Date());
            fmig.run(null);
        } else {
            spark.run(filterList);
            System.out.println("Run follow migrate. " + new Date());
            fmig.run(filterList);
        }

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
