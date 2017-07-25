package com.datastory.banyan.weibo.analyz.util;

import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.serialize.ProtostuffSerializer;

/**
 * com.datastory.banyan.weibo.analyz.util.Util
 *
 * @author lhfcws
 * @since 2017/1/18
 */
public class Util {
    public static StrParams transform(Object o) {
        byte[] bs = (byte[]) o;
        StrParams.StrParamsPojo pojo = ProtostuffSerializer.deserialize(bs, StrParams.StrParamsPojo.class);
        return pojo.getP();
    }

//        public static StrParams transform(Object o) {
//            return (StrParams) o;
//        }
}
