package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.banyan.weibo.analyz.AstroturfingClassifier;
import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Get;

import java.util.Arrays;
import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.component.UTypeComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public class UTypeComponentAnalyzer implements ComponentAnalyzer {
    public static final byte[] R = "r".getBytes();
    public static final byte[] UTA = "user_type_a".getBytes();

    @Override
    public List<String> getInputFields() {
        return Arrays.asList(
                "content", "publish_date", "attitudes_cnt", "comments_cnt", "reposts_cnt"
        );
    }

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        StrParams ret = new StrParams();
        try {
            byte[] pk = BanyanTypeUtil.wbuserPK(uid).getBytes();
            RFieldGetter getter = new RFieldGetter(Tables.ANALYZ_USER_TBL);
            Get get = new Get(pk);
            get.addColumn(R, UTA);
            String uta = getter.singleGet(get, "user_type_a");
            if (StringUtil.isNullOrEmpty(uta))
                return ret;

            String[] items = uta.split(StringUtil.STR_DELIMIT_2ND);
            int userBasicScore = Integer.valueOf(items[0]);
            int contentScore = AstroturfingClassifier.classifyAdvanceBanyan(valueIter);
            int finalScore = AstroturfingClassifier.advanceJudge(userBasicScore, contentScore);
            int utype = WbUserAnalyzer.scavRes2userType(finalScore);
            ret.put("user_type", "" + utype);
        } catch (Exception e) {
            ErrorUtil._LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @Override
    public void setup() {
        LOG.error(this.getClass().getSimpleName() + " does not support streaming processing. ");
    }

    @Override
    public void streamingAnalyz(Object obj) {
    }

    @Override
    public StrParams cleanup() {
        return new StrParams();
    }
}
