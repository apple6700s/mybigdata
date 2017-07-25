package com.datastory.banyan.asyncdata.util;

import com.google.common.collect.HashBiMap;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.ILineParser;

import java.io.IOException;

/**
 * com.datastory.banyan.asyncdata.util.SiteIdMapping
 *
 * @author lhfcws
 * @since 2017/4/20
 */
public class SiteIdMapping {
    private static HashBiMap<String, String> mp = HashBiMap.create();

    static {
        try {
            AdvFile.loadFileInRawLines(ClassUtil.getResourceAsInputStream("video-ecom.site_siteId.csv"), new ILineParser() {
                @Override
                public void parseLine(String s) {
                    String[] arr = s.split(",");
                    mp.put(arr[0], arr[1]);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getSiteName(String siteId) {
        return mp.inverse().get(siteId);
    }

    public  static boolean containsSiteId(String siteId) {
        return mp.inverse().containsKey(siteId);
    }

    public static String getSiteId(String siteName) {
        return mp.get(siteName);
    }

    public static boolean containsSiteName(String siteName) {
        return mp.containsKey(siteName);
    }
}
