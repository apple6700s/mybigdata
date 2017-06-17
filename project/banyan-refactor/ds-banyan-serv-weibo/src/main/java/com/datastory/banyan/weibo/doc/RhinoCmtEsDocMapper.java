package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.RhinoCmtEsDocMapper
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class RhinoCmtEsDocMapper extends ParamsDocMapper {
    public static final String TBL_WBUSER = Tables.table(Tables.PH_WBUSER_TBL);

    public RhinoCmtEsDocMapper(Params in) {
        super(in);
    }

    public static final Map<String, String> RENAME_MAPPING = BanyanTypeUtil.strArr2strMap(new String[]{
            "cmt_mid", "id",
            "uid", "uid",
            "mid", "mid",
            "fingerprint", "fingerprint",
            "content", "content",
            "city", "city",
            "province", "province",
            "name", "user_name",
            "birthyear", "birth_year",
    });

    public static final String[] SHORT_FIELDS = {
            "user_type", "sentiment", "verified_type", "gender",
    };

    public static final String[] LIST_FIELDS = {
            "keywords", "meta_group"
    };

    @Override
    public Params map() {
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, RENAME_MAPPING);
        BanyanTypeUtil.putShortNotNull(out, in, SHORT_FIELDS);
        BanyanTypeUtil.putYzListNotNull(out, in, LIST_FIELDS);

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (in.get("publish_date") != null) {
                    try {
                        Date pDate = DateUtils.parse(in.getString("publish_date"), DateUtils.DFT_TIMEFORMAT);
                        long postTime = pDate.getTime();
                        out.put("comment_time", postTime);
                        out.put("comment_hour", DateUtils.getHourStr(pDate));
                        out.put("comment_date", DateUtils.getDateStr(pDate));
                    } catch (Exception ignore) {
                    }
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (out.getString("content") != null) {
                    out.put("content_length", out.getString("content").length());
                }
                return null;
            }
        });

        out.put("_parent", out.getString("mid"));

        return out;
    }

    static RFieldGetter userReader = new RFieldGetter(TBL_WBUSER);
    public RhinoCmtEsDocMapper fillUser() throws IOException {
        String cmtUid = in.getString("uid");
        Get userGet = new Get(BanyanTypeUtil.wbuserPK(cmtUid).getBytes());
        userGet.addColumn("r".getBytes(), "user_type".getBytes());
        userGet.addColumn("r".getBytes(), "name".getBytes());
        userGet.addColumn("r".getBytes(), "verified_type".getBytes());
        userGet.addColumn("r".getBytes(), "gender".getBytes());
        userGet.addColumn("r".getBytes(), "birthyear".getBytes());
        userGet.addColumn("r".getBytes(), "province".getBytes());
        userGet.addColumn("r".getBytes(), "city".getBytes());
        userGet.addColumn("r".getBytes(), "meta_group".getBytes());

        Map<String, String> cmtUser = userReader.get(userGet);
        if (cmtUser != null) {
            cmtUser.put("uid", cmtUid);
            cmtUser.remove("pk");
            add(cmtUser);
        } else {
            LOG.error("Null comment user : " + cmtUid);
        }
        return this;
    }
}
