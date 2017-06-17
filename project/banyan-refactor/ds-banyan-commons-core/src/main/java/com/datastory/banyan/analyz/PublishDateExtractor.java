package com.datastory.banyan.analyz;

import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * com.datastory.banyan.analyz.PublishDateExtractor
 *
 * @author lhfcws
 * @since 2016/12/26
 */
public class PublishDateExtractor {
    public static Params extract(Params p, String publishDate) {
        if (publishDate == null)
            return p;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DateUtils.DFT_TIMEFORMAT);
            Date date = sdf.parse(publishDate);
            p.put("publish_time", date.getTime());
            p.put("publish_date", publishDate);
            sdf = new SimpleDateFormat(DateUtils.DFT_HOURFORMAT);
            p.put("publish_date_hour", sdf.format(date));
            sdf = new SimpleDateFormat(DateUtils.DFT_DAYFORMAT);
            p.put("publish_date_date", sdf.format(date));
            if (!p.containsKey("publish_date")) {
                p.put("publish_date", publishDate);
            }
        } catch (Exception ignore) {

        }
        return p;
    }
}
