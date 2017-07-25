package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;

import java.util.Arrays;

/**
 * com.datastory.banyan.weibo.hbase.PhoenixWbUserWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class PhoenixWbUserWriter extends PhoenixWriter {
    protected static final String TABLE = Tables.table(Tables.PH_WBUSER_TBL);

    private static volatile PhoenixWbUserWriter _singleton = null;

    public static PhoenixWbUserWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWbUserWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbUserWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixWbUserWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixWbUserWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbUserWriter(num);
                }
            }
        return _singleton;
    }

    private PhoenixWbUserWriter() {
        super(3000);
    }

    public PhoenixWbUserWriter(int cacheSize) {
        super(cacheSize);
    }


    @Override
    protected void init() {
        setFields(Arrays.asList(
                "pk",
                "uid",
                "name",
                "province",
                "city",
                "city_level",
                "desc",
                "blog_url",
                "head_url",
                "url",
                "gender",
//                "birthdate",
//                "birthyear",
//                "constellation",
                "fans_level",
                "fans_cnt",
                "follow_cnt",
                "bi_follow_cnt",
                "wb_cnt",
                "fav_cnt",
                "publish_date",
                "update_date",
                "location",
                "domain",
                "weihao",
                "other_data",
                "verified_type",
                "vtype",
//                "user_type",
                "last_tweet_date"
        ));

//        if (Tables.isEnablePK())
//            hooks.add(WeiboUserKafkaProducer.getInstance(getTable()));
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
