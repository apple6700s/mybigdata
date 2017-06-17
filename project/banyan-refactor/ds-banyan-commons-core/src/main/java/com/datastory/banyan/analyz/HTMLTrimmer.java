package com.datastory.banyan.analyz;

import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;

import java.io.Serializable;
import java.util.Date;

/**
 * com.datastory.banyan.analyz.HTMLTrimmer
 *
 * @author lhfcws
 * @since 16/11/29
 */

public class HTMLTrimmer implements Serializable {
    public static final String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>";
    public static final String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>";
    public static final String regEx_html = "<[^>]+>";


    public static String trim(String s) {
        if (s == null) return null;
        return s.replaceAll(regEx_style, " ").replaceAll(regEx_script, " ")
                .replaceAll(regEx_html, " ").replaceAll("[ ]+", " ");
    }

    public static Params trim(Params p, String key) {
        if (p != null && p.get(key) != null) {
            p.put(key, trim(p.getString(key)));
        }
        return p;
    }

    public static StrParams trim(StrParams p, String key) {
        if (p != null && !StringUtil.isNullOrEmpty(p.get(key))) {
            p.put(key, trim(p.get(key)));
        }
        return p;
    }

    /**
     * test main
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        String s = "回复：<a href=\\\"http://jump.bdimg.com/safecheck/index?url=x+Z5mMbGPAvVIlwZePSt0B3tEqEFWbC4kYEIagYmMtZZX2ReJf/lyE49zhlykq5iJoow2wxfK0E5KiP1BCpYOtYusA3Y57dRNv8XwVyYmFe6UrDvpvPBtP9VGC1JW30OmSULyB9JvmQdsArlBBE2HcpVpkBMHWFYNUHIFUv6ADbGd+gKNYxfv9vjSS6TR1mcCsYnXs4/O9JVdtZX5QDs0shMZgfTf+uS\\\"  target=\\\"_blank\\\">http://pan.baidu.com/share/home?uk=1882461099#category/type=0?jF=%E9%98%9C%E9%9B%8C%E5%B3%A6%E9%9C%B8%E8%8D%92%E8%A1%A</a>";
        System.out.println(trim(s));

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
