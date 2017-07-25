package com.datastory.banyan.newsforum.doc;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.doc.TrendDocMapper;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.util.StringUtils;

/**
 * com.datastory.banyan.newsforum.doc.NFTrendDocMapper
 *
 * @author lhfcws
 * @since 2017/7/5
 */
public class NFTrendDocMapper extends TrendDocMapper {
    public NFTrendDocMapper(Params in) {
        super(in, RhinoETLConsts.SRC_NF, "cnt", "parent_post_id", "pk");
    }

    @Override
    /**
     * pk           nf|${id}|cnt
     * source       nf
     * id           ${id}
     * parent_id    ${parent_id}    // 有父贴的才有
     * type         cnt
     * update_date  20170706101908  // yyyyMMddHHmmsss
     * data         |0||        // view_cnt|review_cnt|like_cnt|dislike_cnt
     */
    // ""代表不存在
    protected String getData() {
        return StringUtils.join("|", new String[] {
                getIntStr("view_cnt"),
                getIntStr("review_cnt"),
                getIntStr("like_cnt"),
                getIntStr("dislike_cnt")
        });
    }

    private String getIntStr(String key) {
        if (in.get(key) == null)
            return "";
        try {
            int v = Integer.parseInt(in.getString(key));
            return v + "";
        } catch (Exception ignore) {
            return "";
        }
    }
}
