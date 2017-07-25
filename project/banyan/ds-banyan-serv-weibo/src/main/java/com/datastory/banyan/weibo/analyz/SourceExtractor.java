package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.reflect.TypeToken;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.SourceExtractor
 *
 * @author lhfcws
 * @since 2016/12/22
 */
public class SourceExtractor {
    static Type LIST_STR = new TypeToken<List<String>>() {
    }.getType();
    static Type LIST_MAP = new TypeToken<StrParams>() {
    }.getType();

    public static List<String> extractSources(String sourceStr) {
        if (StringUtil.isNullOrEmpty(sourceStr))
            return null;
        if (sourceStr.startsWith("{")) {
            StrParams sourceMap = FastJsonSerializer.deserialize(sourceStr, StrParams.class);
            String source = sourceMap.get("name");
            return Collections.singletonList(source);
        } else if (sourceStr.startsWith("[")) {
            try {
                List<String> list1 = FastJsonSerializer.deserialize(sourceStr, LIST_STR);
                List<String> ret = new ArrayList<>();
                for (String s : list1) {
                    ret.add(s.split("\\$")[0]);
                }
                return ret;
            } catch (Exception e1) {
                try {
                    List<StrParams> list2 = FastJsonSerializer.deserialize(sourceStr, LIST_MAP);
                    List<String> ret = new ArrayList<>();
                    for (StrParams m : list2)
                        if (m.get("name") != null) {
                            ret.add(m.get("name"));
                        }
                    return ret;
                } catch (Exception e2) {
                }
            }
        } else {
            return BanyanTypeUtil.yzStr2List(sourceStr);
        }
        return null;
    }
}
